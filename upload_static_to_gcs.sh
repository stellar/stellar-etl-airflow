#!/bin/bash

set -eo pipefail

airflow_bucket=${1}

if [ -z "${airflow_bucket}" ]; then
    echo "$0 is a script that uploads the Airflow dags and BigQuery schemas to Google Cloud Storage"
    echo "Usage: $0 <airflow_bucket>"
    exit 1
fi

gsutil -m cp -r dags/* gs://"${airflow_bucket}"/dags/
gsutil -m cp -r schemas/* gs://"${airflow_bucket}"/dags/schemas/
gsutil cp dags/stellar_etl_airflow/macros.py gs://"${airflow_bucket}"/plugins/macros.py

