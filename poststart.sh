#!/bin/bash -e
# Background process that copies extracts to gcsfuse mount on airflow-workers
#
# Author: Debnil Sur
# Date: October 2020 
#
# This script is a long running background process on the airflow-worker
# nodes. It listens for the existing of new files in the /home/airflow/etlData
# directory, which is where data is exported using the stellar-etl framework.
# If the file is complete and closed, the script moves the file to the correct
# gcsfused mount, meaning that it is copied across all workers so that downstream
# Airflow tasks can reference the file.
#
# Follow the directions in the README.md to get the script mapped and executing
# on the airflow-worker nodes.

move_file_if_closed() {
    preFileSize=$(wc -c < "$1")
    sleep 10;
    postFileSize=$(wc -c < "$1")
    lsof "$1";
    # if lsof returns an error code of 1, it means the file is not opened by any 
    # other processes, allowing us to move it safely. Confirm there is data first.
    if [[ $? == 1 && $preFileSize -gt 0]]; then
        # File size is measured 10 seconds apart to confirm any batch process finished
        # writing to the file. If the sizes do not match, the file is still in use.
        if [[ $preFileSize == $postFileSize ]]; then
            mv "$1" /home/airflow/gcs/data/;
        fi
    fi
}

sudo apt-get update && sudo apt-get install lsof;
while true; do
    for file in /home/airflow/etlData/*
    do
        if [[ -f "$file" ]]; then
            move_file_if_closed "$file"
        else
            for changeFile in "$file"/*
            do
                if [[ -f "$changeFile" ]]; then
                    move_file_if_closed "$changeFile"
            fi
            done
        fi
    done
    sleep 1;
done >/dev/null 2>&1 &
disown
disown -a
exit 0
