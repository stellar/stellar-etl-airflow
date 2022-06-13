# /bin/bash -e 
#
#
###############################################################################
# Ad Hoc script to delete any old GCS files 
#
# Author: Sydney Wiseman
# Date: 26 May 2022
#
# Script will loop through batch run dates and delete any old files that
# match a specified pattern. To be used in instances of needing to rerun
# large amounts of data for the state tables. Since state tables load data
# via pattern matching, the old data must be removed first.
#
# User can pass month number, day number and pattern string to match.
###############################################################################
 
PATTERN=$1
YEAR=$2
MONTH=$3
START_DAY=$4
END_DAY=$5

GCS_BUCKET=gs://us-central1-hubble-2-d948d67b-bucket/dag-exported/scheduled__

# make state tables
for day in $(seq $START_DAY $END_DAY)
do 
    echo "Removing files matching pattern $PATTERN for month $MONTH and day $day"
    if [ $day -lt 10 ]; then
        gsutil -m rm -rf $GCS_BUCKET$YEAR-$MONTH-0$day*/$PATTERN* 
    elif [ $day -ge 10 ]; then
        gsutil -m rm -rf $GCS_BUCKET$YEAR-$MONTH-$day*/$PATTERN* 
    fi
done
