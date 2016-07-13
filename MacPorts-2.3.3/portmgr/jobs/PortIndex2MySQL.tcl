#!@TCLSH@
# -*- coding: utf-8; mode: tcl; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- vim:fenc=utf-8:filetype=tcl:et:sw=4:ts=4:sts=4
#
# PortIndex2MySQL.tcl
# Kevin Van Vechten | kevin@opendarwin.org
# 3-Oct-2002
# Juan Manuel Palacios | jmpp@macports.org
# 22-Nov-2007
# $Id: PortIndex2MySQL.tcl 119170 2014-04-18 21:57:35Z cal@macports.org $
#
# Copyright (c) 2007 Juan Manuel Palacios, The MacPorts Project.
# Copyright (c) 2003 Apple Inc.
# Copyright (c) 2002 Kevin Van Vechten. 
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of Apple Inc. nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


#####
# The PortIndex2MySQL script populates a database with key information extracted
# from the Portfiles in the ports tree pointed to by the sources.conf file in a
# MacPorts installation, found by loading its macports1.0 tcl package and initializing
# it with 'mportinit' below. Main use of the resulting database is providing live
# information to the ports.php page, a client tailored to poll it. For this very reason,
# information fed to the database always has to be kept up to date in order to remain
# meaningful, which is accomplished simply by calling the 'mportsync' proc in macports1.0
# (which updates the ports tree in use) and by installing the script on cron/launchd to be
# run on a timely schedule (not any more frequent than the run of the PortIndexRegen.sh
# script on that creates a new PortIndex file).
#
# Remaining requirement to successfully run this script is performing the necessary
# MySQL admin tasks on the host box to create the database in the first place and the
# MySQL user that will be given enough privileges to alter it. Values in the database
# related variables provided below have to be adapted accordingly to match the chosen
# setup.
#####



# Runtime information log file and reciepient.
set runlog "/tmp/portsdb.log"
set runlog_fd [open $runlog w+]
set lockfile "/tmp/portsdb.lock"
set mailprog "/usr/sbin/sendmail"
set DATE [clock format [clock seconds] -format "%A %Y-%m-%d at %T"]

#set SPAM_LOVERS example@hostname.com

set SUBJECT "PortIndex2MySQL run failure on $DATE"
set FROM macports-mgr@lists.macosforge.org
set HEADERS "To: $SPAM_LOVERS\r\nFrom: $FROM\r\nSubject: $SUBJECT\r\n\r\n"

# handle command line arguments
set create_tables true
if {[llength $argv]} {
    if {[lindex $argv 0] eq "--create-tables"} {
        set create_tables true
    }
}

# House keeping on exit.
proc cleanup {args} {
    foreach file_to_clean $args {
        upvar $file_to_clean up_file_to_clean
        upvar ${file_to_clean}_fd up_file_to_clean_fd
        close $up_file_to_clean_fd
        file delete -force $up_file_to_clean
    }
}

# What to do when terminating execution, depending on the $exit_status condition.
proc terminate {exit_status} {
    global runlog runlog_fd
    if {$exit_status} {
        global subject SPAM_LOVERS mailprog
        seek $runlog_fd 0 start
        exec -- $mailprog $SPAM_LOVERS <@ $runlog_fd
    }
    cleanup runlog
    exit $exit_status
}

# macports1.0 UI instantiation to route information/error messages wherever we want.
# This is a custom ui_channels proc because we want to get reported information on
# channels other than the default stdout/stderr that the macports1.0 API provides,
# namely a log file we can later mail to people in charge if need be.
proc ui_channels {priority} {
    global runlog_fd
    switch $priority {
        debug {
            if {[macports::ui_isset ports_debug]} {
                return $runlog_fd
            } else {
                return {}
            }
        }
        info {
            if {[macports::ui_isset ports_verbose]} {
                return $runlog_fd
            } else {
                return {}
            }
        }
        msg {
            if {[macports::ui_isset ports_quiet]} {
                return $runlog_fd
            } else {
                return {}
            }
        }
        error {
            return $runlog_fd
        }
        default {
            return {}
        }
    }
}

# Procedure to catch the database password from a protected file.
proc getpasswd {passwdfile} {
    if {[catch {open $passwdfile r} passwdfile_fd]} {
        global lockfile lockfile_fd
        ui_error "${::errorCode}: $passwdfile_fd"
        cleanup lockfile
        terminate 1
    }
    if {[gets $passwdfile_fd passwd] <= 0} {
        global lockfile lockfile_fd
        close $passwdfile_fd
        ui_error "No password found in password file $passwdfile!"
        cleanup lockfile
        terminate 1
    }
    close $passwdfile_fd
    return $passwd
}

# SQL string escaping.
proc sql_escape {str} {
    regsub -all -- {'} $str {\\'} str
    regsub -all -- {"} $str {\\"} str
    regsub -all -- {\n} $str {\\n} str
    return $str
}

# We first initialize the runlog with proper mail headers
puts $runlog_fd $HEADERS

# Check if there are any stray sibling jobs before moving on, bail in such case.
if {[file exists $lockfile]} {
    puts $runlog_fd "PortIndex2MySQL lock file found, is another job running?" 
    terminate 1
} else {
    set lockfile_fd [open $lockfile a]
}

# Load macports1.0 so that we can use some of its procs and the portinfo array.
if {[catch { package require macports } errstr]} {
    puts $runlog_fd "${::errorInfo}"
    puts $runlog_fd "Failed to load the macports1.0 Tcl package: $errstr"
    cleanup lockfile
    terminate 1
}

# Initialize macports1.0 and its UI, in order to find the sources.conf file
# (which is what will point us to the PortIndex we're gonna use) and use
# the runtime information.
array set ui_options {ports_verbose yes}
if {[catch {mportinit ui_options} errstr]} {
    puts $runlog_fd "${::errorInfo}"
    puts $runlog_fd "Failed to initialize MacPorts: $errstr"
    cleanup lockfile
    terminate 1
}


# Database abstraction variables:
set sqlfile "/tmp/portsdb.sql"
set portsdb_host localhost
set portsdb_name macports
set portsdb_user macports
set passwdfile "/opt/local/share/macports/resources/portmgr/password_file"
set portsdb_passwd [getpasswd $passwdfile]
set portsdb_cmd [macports::findBinary mysql5]


# Flat text file to which sql statements are written.
if {[catch {open $sqlfile w+} sqlfile_fd]} {
    ui_error "${::errorCode}: $sqlfile_fd"
    cleanup lockfile
    terminate 1
}


# Call the sync procedure to make sure we always have a fresh ports tree.
if {[catch {mportsync} errstr]} {
    ui_error "${::errorInfo}"
    ui_error "Failed to update the ports tree, $errstr"
    cleanup sqlfile lockfile
    terminate 1
}

# Load every port in the index through a search that matches everything.
if {[catch {set ports [mportlistall]} errstr]} {
    ui_error "${::errorInfo}"
    ui_error "port search failed: $errstr"
    cleanup sqlfile lockfile
    terminate 1
}

if {$create_tables} {
    # Initial creation of database tables: log, portfiles, categories, maintainers, dependencies, variants and platforms.
    # Do we need any other?
    puts $sqlfile_fd "DROP TABLE IF EXISTS log;"
    puts $sqlfile_fd "CREATE TABLE log (activity VARCHAR(255), activity_time TIMESTAMP(14)) DEFAULT CHARSET=utf8;"
    
    puts $sqlfile_fd "DROP TABLE IF EXISTS portfiles;"
    puts $sqlfile_fd "CREATE TABLE portfiles (name VARCHAR(255) PRIMARY KEY NOT NULL, path VARCHAR(255), version VARCHAR(255),  description TEXT) DEFAULT CHARSET=utf8;"
    
    puts $sqlfile_fd "DROP TABLE IF EXISTS categories;"
    puts $sqlfile_fd "CREATE TABLE categories (portfile VARCHAR(255), category VARCHAR(255), is_primary INTEGER) DEFAULT CHARSET=utf8;"
    
    puts $sqlfile_fd "DROP TABLE IF EXISTS maintainers;"
    puts $sqlfile_fd "CREATE TABLE maintainers (portfile VARCHAR(255), maintainer VARCHAR(255), is_primary INTEGER) DEFAULT CHARSET=utf8;"
    
    puts $sqlfile_fd "DROP TABLE IF EXISTS dependencies;"
    puts $sqlfile_fd "CREATE TABLE dependencies (portfile VARCHAR(255), library VARCHAR(255)) DEFAULT CHARSET=utf8;"
    
    puts $sqlfile_fd "DROP TABLE IF EXISTS variants;"
    puts $sqlfile_fd "CREATE TABLE variants (portfile VARCHAR(255), variant VARCHAR(255)) DEFAULT CHARSET=utf8;"
    
    puts $sqlfile_fd "DROP TABLE IF EXISTS platforms;"
    puts $sqlfile_fd "CREATE TABLE platforms (portfile VARCHAR(255), platform VARCHAR(255)) DEFAULT CHARSET=utf8;"

    puts $sqlfile_fd "DROP TABLE IF EXISTS licenses;"
    puts $sqlfile_fd "CREATE TABLE licenses (portfile VARCHAR(255), license VARCHAR(255)) DEFAULT CHARSET=utf8;"
} else {
    # if we are not creating tables from scratch, remove the old data
    puts $sqlfile_fd "TRUNCATE log;"
    puts $sqlfile_fd "TRUNCATE portfiles;"
    puts $sqlfile_fd "TRUNCATE categories;"
    puts $sqlfile_fd "TRUNCATE maintainers;"
    puts $sqlfile_fd "TRUNCATE dependencies;"
    puts $sqlfile_fd "TRUNCATE variants;"
    puts $sqlfile_fd "TRUNCATE platforms;"
    puts $sqlfile_fd "TRUNCATE licenses;"
}
 
# Iterate over each matching port, extracting its information from the
# portinfo array.
foreach {name array} $ports {

    array unset portinfo
    array set portinfo $array

    set portname [sql_escape $portinfo(name)]
    if {[info exists portinfo(version)]} {
        set portversion [sql_escape $portinfo(version)]
    } else {
        set portversion ""
    }
    set portdir [sql_escape $portinfo(portdir)]
    if {[info exists portinfo(description)]} {
        set description [sql_escape $portinfo(description)]
    } else {
        set description ""
    }
    if {[info exists portinfo(categories)]} {
        set categories $portinfo(categories)
    } else {
        set categories ""
    }
    if {[info exists portinfo(maintainers)]} {
        set maintainers $portinfo(maintainers)
    } else {
        set maintainers ""
    }
    if {[info exists portinfo(variants)]} {
        set variants $portinfo(variants)
    } else {
        set variants ""
    }
    if {[info exists portinfo(depends_fetch)]} {
        set depends_fetch $portinfo(depends_fetch)
    } else {
        set depends_fetch ""
    }
    if {[info exists portinfo(depends_extract)]} {
        set depends_extract $portinfo(depends_extract)
    } else {
        set depends_extract ""
    }
    if {[info exists portinfo(depends_build)]} {
        set depends_build $portinfo(depends_build)
    } else {
        set depends_build ""
    }
    if {[info exists portinfo(depends_lib)]} {
        set depends_lib $portinfo(depends_lib)
    } else {
        set depends_lib ""
    }
    if {[info exists portinfo(depends_run)]} {
        set depends_run $portinfo(depends_run)
    } else {
        set depends_run ""
    }
    if {[info exists portinfo(platforms)]} {
        set platforms $portinfo(platforms)
    } else {
        set platforms ""
    }
    if {[info exists portinfo(license)]} {
        set licenses $portinfo(license)
    } else {
        set licenses ""
    }

    puts $sqlfile_fd "INSERT INTO portfiles VALUES ('$portname', '$portdir', '$portversion', '$description');"

    set primary 1
    foreach category $categories {
        set category [sql_escape $category]
        puts $sqlfile_fd "INSERT INTO categories VALUES ('$portname', '$category', $primary);"
        set primary 0
    }
    
    set primary 1
    foreach maintainer $maintainers {
        set maintainer [sql_escape $maintainer]
        puts $sqlfile_fd "INSERT INTO maintainers VALUES ('$portname', '$maintainer', $primary);"
        set primary 0
    }

    foreach fetch_dep $depends_fetch {
        set fetch_dep [sql_escape $fetch_dep]
        puts $sqlfile_fd "INSERT INTO dependencies VALUES ('$portname', '$fetch_dep');"
    }
    
    foreach extract_dep $depends_extract {
        set extract_dep [sql_escape $extract_dep]
        puts $sqlfile_fd "INSERT INTO dependencies VALUES ('$portname', '$extract_dep');"
    }

    foreach build_dep $depends_build {
        set build_dep [sql_escape $build_dep]
        puts $sqlfile_fd "INSERT INTO dependencies VALUES ('$portname', '$build_dep');"
    }

    foreach lib $depends_lib {
        set lib [sql_escape $lib]
        puts $sqlfile_fd "INSERT INTO dependencies VALUES ('$portname', '$lib');"
    }

    foreach run_dep $depends_run {
        set run_dep [sql_escape $run_dep]
        puts $sqlfile_fd "INSERT INTO dependencies VALUES ('$portname', '$run_dep');"
    }

    foreach variant $variants {
        set variant [sql_escape $variant]
        puts $sqlfile_fd "INSERT INTO variants VALUES ('$portname', '$variant');"
    }

    foreach platform $platforms {
        set platform [sql_escape $platform]
        puts $sqlfile_fd "INSERT INTO platforms VALUES ('$portname', '$platform');"
    }

    foreach license $licenses {
        set license [sql_escape $license]
        puts $sqlfile_fd "INSERT INTO licenses VALUES ('$portname', '$license');"
    }

}

# Mark the db regen as done only once we're done processing all ports:
puts $sqlfile_fd "INSERT INTO log VALUES ('update', NOW());"

# Pipe the contents of the generated sql file to the database command,
# reading from the file descriptor for the raw sql file to assure completeness.
if {[catch {seek $sqlfile_fd 0 start} errstr]} {
    ui_error "${::errorCode}: $errstr"
    cleanup sqlfile lockfile
    terminate 1
}

if {[catch {exec -- $portsdb_cmd --host=$portsdb_host --user=$portsdb_user --password=$portsdb_passwd --database=$portsdb_name <@ $sqlfile_fd} errstr]} {
    ui_error "${::errorCode}: $errstr"
    cleanup sqlfile lockfile
    terminate 1
}

# done regenerating the database. Cleanup and exit successfully.
cleanup sqlfile lockfile
terminate 0
