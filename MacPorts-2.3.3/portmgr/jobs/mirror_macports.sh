#!/bin/bash
####
# Run "port mirror" for each variant of each port 
# Created by William Siegrist,
# e-mail: wsiegrist@apple.com
# $Id: mirror_macports.sh 84526 2011-09-27 18:05:09Z jmr@macports.org $
####

# regexp of ports that we do not mirror
EXCLUSIONS='^(molden|metis)$'

# macports really wants this, so lets appease it
export COLUMNS=80
export LINES=24

# send all output here
LOG="/admin/var/log/macports.log"

PORT="/opt/local/bin/port"
CUT=/usr/bin/cut
GREP=/usr/bin/grep
XARGS=/usr/bin/xargs
EGREP=/usr/bin/egrep

exec >> $LOG 2>&1

echo "------------------------------
  Beginning mirror run 
------------------------------";

# for each port
for P in `$PORT list | $CUT -f 1 -d " " | $EGREP -v $EXCLUSIONS`;
do

  NOW=`/bin/date`;
  echo "TIME: ${NOW}";

  # mirror with no variants
  echo "Mirroring ${P}";
  $PORT clean $P;
  $PORT mirror $P;

  # for each variant
  for V in `$PORT -q variants $P | $CUT -d " " -f 1 | $CUT -d ":" -f 1 | $GREP -v universal | $XARGS`;
  do
    # mirror with each variant
    echo "Mirroring ${P} +${V}";
    $PORT clean $P; 
    $PORT mirror $P +$V; 
  done

  # mirror with each platform (can exclude the one the server is running)
  for VERS in "8 9 10";
  do
    for ARCH in "i386 powerpc";
    do
      echo "Mirroring ${P} with platform darwin ${VERS} ${ARCH}"
      $PORT mirror $P os.major=${VERS} os.arch=${ARCH}
    done
  done

  # clean up the work area
  $PORT clean --work $P;

done

# record the last time we mirrored
/bin/date > /rsync/macports-san/distfiles/TIMESTAMP

echo "------------------------------
  End of mirror run 
------------------------------";

