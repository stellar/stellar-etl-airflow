#!/bin/bash

move_file_if_closed() {
    lsof "$1";
    # if lsof returns an error code of 1, it means the file is not opened by any other processes, allowing us to move it safely
    if (($? == 1)); then
        mv "$1" /home/airflow/gcs/data/;
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
