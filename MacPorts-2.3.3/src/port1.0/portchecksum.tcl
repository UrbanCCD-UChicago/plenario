# et:ts=4
# portchecksum.tcl
# $Id: portchecksum.tcl 117049 2014-02-14 00:15:07Z jmr@macports.org $
#
# Copyright (c) 2002 - 2004 Apple Inc.
# Copyright (c) 2004 - 2005 Paul Guyot <pguyot@kallisys.net>
# Copyright (c) 2006-2011 The MacPorts Project
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
#

package provide portchecksum 1.0
package require portutil 1.0

set org.macports.checksum [target_new org.macports.checksum portchecksum::checksum_main]
target_provides ${org.macports.checksum} checksum
target_requires ${org.macports.checksum} main fetch
target_prerun ${org.macports.checksum} portchecksum::checksum_start

namespace eval portchecksum {
}

# Options
options checksums checksum.skip

# Defaults
default checksums ""
default checksum.skip false

set_ui_prefix

# The list of the types of checksums we know.
set checksum_types [list md5 sha1 rmd160 sha256 size]

# The number of types we know.
set checksum_types_count [llength $checksum_types]

# types to recommend if none are specified in the portfile
set default_checksum_types [list rmd160 sha256]

# Using global all_dist_files, parse the checksums and store them into the
# global array checksums_array.
#
# There are two formats:
# type value [type value [type value]]                      for a single file
# file1 type value [type value [type value]] [file2 ...]    for multiple files.
#
# Portfile is in format #1 if:
# (1) There is only one distfile.
# (2) There are an even number of words in checksums (i.e. "md5 cksum sha1 cksum" = 4 words).
# (3) There are no more than $checksum_types_count checksums specified.
# (4) first word is one of the checksums types.
#
# return yes if the syntax was correct, no if there was a problem.
proc portchecksum::parse_checksums {checksums_str} {
    global checksums_array all_dist_files checksum_types checksum_types_count

    # Parse the string of checksums.
    set nb_checksum [llength $checksums_str]

    if {[llength $all_dist_files] == 1
        && [expr {$nb_checksum % 2}] == 0
        && [expr {$nb_checksum / 2}] <= $checksum_types_count
        && [lsearch -exact $checksum_types [lindex $checksums_str 0]] >= 0} {
        # Convert to format #2
        set checksums_str [linsert $checksums_str 0 [lindex $all_dist_files 0]]
        # We increased the size.
        incr nb_checksum
    }

    # Create the array with the checksums.
    array set checksums_array {}

    set result yes

    # Catch out of bounds errors (they're syntax errors).
    if {[catch {
        # Parse the string as if it was in format #2.
        for {set ix_checksum 0} {$ix_checksum < $nb_checksum} {incr ix_checksum} {
            # first word is the file.
            set checksum_filename [lindex $checksums_str $ix_checksum]

            # retrieve the list of values we already know for this file.
            set checksum_values {}
            if {[info exists checksums_array($checksum_filename)]} {
                set checksum_values $checksums_array($checksum_filename)
            }

            # append the new value
            incr ix_checksum
            while {1} {
                set checksum_type [lindex $checksums_str $ix_checksum]
                if {[lsearch -exact $checksum_types $checksum_type] >= 0} {
                    # append the type and the value.
                    incr ix_checksum
                    set checksum_value [lindex $checksums_str $ix_checksum]
                    incr ix_checksum

                    lappend checksum_values $checksum_type
                    lappend checksum_values $checksum_value
                } else {
                    # this wasn't a type but the next dist file.
                    incr ix_checksum -1
                    break
                }

                # stop if we exhausted all the items in the list.
                if {$ix_checksum == $nb_checksum} {
                    break
                }
            }

            # set the values in the array.
            set checksums_array($checksum_filename) $checksum_values
        }
    } error]} {
        # An error occurred.
        global errorInfo
        ui_debug "$errorInfo"
        ui_error "Couldn't parse checksum line ($checksums_str) [$error]"

        # Something wrong happened.
        set result no
    }

    return $result
}

# calc_md5
#
# Calculate the md5 checksum for the given file.
# Return the checksum.
#
proc portchecksum::calc_md5 {file} {
    return [md5 file $file]
}

# calc_sha1
#
# Calculate the sha1 checksum for the given file.
# Return the checksum.
#
proc portchecksum::calc_sha1 {file} {
    return [sha1 file $file]
}

# calc_rmd160
#
# Calculate the rmd160 checksum for the given file.
# Return the checksum.
#
proc portchecksum::calc_rmd160 {file} {
    return [rmd160 file $file]
}

# calc_sha256
#
# Calculate the sha256 checksum for the given file.
# Return the checksum.
#
proc portchecksum::calc_sha256 {file} {
    return [sha256 file $file]
}

# calc_size
#
# Get the size of the given file.
# Return the size.
#
proc portchecksum::calc_size {file} {
    return [file size $file]
}

# checksum_start
#
# Target prerun procedure; simply prints a message about what we're doing.
#
proc portchecksum::checksum_start {args} {
    global UI_PREFIX

    ui_notice "$UI_PREFIX [format [msgcat::mc "Verifying checksums for %s"] [option subport]]"
}

# checksum_main
#
# Target main procedure. Verifies the checksums of all distfiles.
#
proc portchecksum::checksum_main {args} {
    global UI_PREFIX all_dist_files checksum_types checksums_array portverbose checksum.skip \
           usealtworkpath altprefix default_checksum_types

    # If no files have been downloaded, there is nothing to checksum.
    if {![info exists all_dist_files]} {
        return 0
    }

    # Completely bypass checksumming if checksum.skip=yes
    # This should be considered an extreme measure
    if {[tbool checksum.skip]} {
        ui_info "$UI_PREFIX Skipping checksum phase"
        return 0
    }

    # so far, everything went fine.
    set fail no

    # Set the list of checksums as the option checksums.
    set checksums_str [option checksums]

    # store the calculated checksums to avoid repeated calculations
    set sums ""

    # if everything is fine with the syntax, keep on and check the checksum of
    # the distfiles.
    if {[parse_checksums $checksums_str] eq "yes"} {
        set distpath [option distpath]

        foreach distfile $all_dist_files {
            ui_info "$UI_PREFIX [format [msgcat::mc "Checksumming %s"] $distfile]"

            # get the full path of the distfile.
            set fullpath [file join $distpath $distfile]
            if {![file isfile $fullpath]} {
                if {!$usealtworkpath && [file isfile "${altprefix}${fullpath}"]} {
                    set fullpath "${altprefix}${fullpath}"
                } else {
                    return -code error "$distfile does not exist in $distpath"
                }
            }

            if {[llength $all_dist_files] > 1} {
                lappend sums $distfile
            }

            # check that there is at least one checksum for the distfile.
            if {![info exists checksums_array($distfile)] || [llength $checksums_array($distfile)] < 1} {
                ui_error "[format [msgcat::mc "No checksum set for %s"] $distfile]"
                set fail yes

                # no checksums specified; output the default set
                foreach type $default_checksum_types {
                    lappend sums [format "%-8s%s" $type [calc_$type $fullpath]]
                }

            } else {
                # retrieve the list of types/values from the array.
                set portfile_checksums $checksums_array($distfile)

                # iterate on this list to check the actual values.
                foreach {type sum} $portfile_checksums {
                    set calculated_sum [calc_$type $fullpath]
                    lappend sums [format "%-8s%s" $type $calculated_sum]

		    # Used for regression testing
                    ui_debug "[format [msgcat::mc "Calculated (%s) is %s"] $type $calculated_sum]"

                    if {$sum eq $calculated_sum} {
                        ui_debug "[format [msgcat::mc "Correct (%s) checksum for %s"] $type $distfile]"
                    } else {
                        ui_error "[format [msgcat::mc "Checksum (%s) mismatch for %s"] $type $distfile]"
                        ui_info_fetch "[format [msgcat::mc "Portfile checksum: %s %s %s"] $distfile $type $sum]"
                        ui_info "[format [msgcat::mc "Distfile checksum: %s %s %s"] $distfile $type $calculated_sum]"

                        # Raise the failure flag
                        set fail yes
                    }
                }
                if {[tbool fail] && ![regexp {\.html?$} ${distfile}] &&
                    ![catch {strsed [exec [findBinary file $portutil::autoconf::file_path] $fullpath --brief --mime] {s/;.*$//}} mimetype]
                    && "text/html" == $mimetype} {
                    # file --mime-type would be preferable to file --mime and strsed, but is only available as of Snow Leopard
                    set wrong_mimetype yes
                    set htmlfile_path ${fullpath}.html
                    file rename -force $fullpath $htmlfile_path
                }
            }

        }
    } else {
        # Something went wrong with the syntax.
        set fail yes
    }

    if {[tbool fail]} {

        if {[tbool wrong_mimetype]} {
            # We got an HTML file, though the distfile name does not suggest that one was
            # expected. Probably a helpful DNS server sent us to its search results page
            # instead of admitting that the server we asked for doesn't exist, or a mirror that
            # no longer has the file served its error page with a 200 response.
            ui_notice "***"
            ui_notice "The non-matching file appears to be HTML. See this page for possible reasons"
            ui_notice "for the checksum mismatch:"
            ui_notice "<https://trac.macports.org/wiki/MisbehavingServers>"
            ui_notice "***"
            ui_notice "The file has been moved to: $htmlfile_path"
        } else {
            # Show the desired checksum line for easy cut-paste
            ui_info "The correct checksum line may be:"
            ui_info [format "%-20s%s" "checksums" [join $sums [format " \\\n%-20s" ""]]]
        }

        return -code error "[msgcat::mc "Unable to verify file checksums"]"
    }

    return 0
}
