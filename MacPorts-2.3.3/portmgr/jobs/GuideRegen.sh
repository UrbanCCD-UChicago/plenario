#!/bin/bash

####
# Guide regen automation script.
# Created by Daniel J. Luke
# e-mail: dluke@geeklair.net
# Based on IndexRegen.sh
# $Id: GuideRegen.sh 79027 2011-05-30 22:19:44Z jmr@macports.org $
####

# Configuration
LOCKFILE=/tmp/.mp_svn_guide_regen.lock
# ROOT directory, where everything is. This needs to exist!
ROOT=/var/tmp/macports/
# e-mail address to spam in case of failure.
#SPAM_LOVERS=example@hostname.com

# Other settings (probably don't need to be changed).
SVN_CONFIG_DIR=${ROOT}/svnconfig
REPO_BASE=https://svn.macports.org/repository/macports
SVN="/opt/local/bin/svn -q --non-interactive --config-dir $SVN_CONFIG_DIR"
# Where to checkout the source code. This needs to exist!
SRCTREE=${ROOT}/source
# Log for the e-mail in case of failure.
FAILURE_LOG=${ROOT}/guide_failure.log
# The date.
DATE=$(/bin/date +'%A %Y-%m-%d at %H:%M:%S')

# Where to find the binaries we need
MAIL=/usr/bin/mail
RM=/bin/rm
TOUCH=/usr/bin/touch
MAKE=/usr/bin/make
MKDIR=/bin/mkdir

# Function to spam people in charge if something goes wrong during guide regen.
bail () {
    $MAIL -s "Guide Regen Failure on ${DATE}" $SPAM_LOVERS < $FAILURE_LOG
    cleanup; exit 1
}

# Cleanup fuction for runtime files.
cleanup () {
    $RM -f $FAILURE_LOG $LOCKFILE
}


if [ ! -e $LOCKFILE ]; then
    $TOUCH $LOCKFILE
else
    echo "Guide Regen lockfile found, is another regen job running?" > $FAILURE_LOG; bail
fi

# Checkout/update the doc tree
if [ -d ${SRCTREE}/doc-new ]; then
    $SVN update ${SRCTREE}/doc-new > $FAILURE_LOG 2>&1 \
        || { echo "Updating the doc tree from $REPO_BASE/trunk/doc-new failed." >> $FAILURE_LOG; bail ; }
else
    $MKDIR -p ${SRCTREE}/doc-new
    $SVN checkout ${REPO_BASE}/trunk/doc-new ${SRCTREE}/doc-new > $FAILURE_LOG 2>&1 \
        || { echo "Checking out the doc tree from $REPO_BASE/trunk/doc-new failed." >> $FAILURE_LOG; bail ; }
fi

# build single html version
{ cd ${SRCTREE}/doc-new && $MAKE guide > $FAILURE_LOG 2>&1 ; } \
    || { echo "make failed." >> $FAILURE_LOG ; bail ; }

# build chunked version 
{ cd ${SRCTREE}/doc-new && $MAKE guide-chunked > $FAILURE_LOG 2>&1 ; } \
    || { echo "make failed." >> $FAILURE_LOG ; bail ; }

# At this point the guide was regen'd successfuly, so we cleanup before we exit.
cleanup && exit 0
