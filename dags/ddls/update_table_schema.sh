# bin/bash -e 

PROJECT_ID = test-hubble-319619
DATASET_ID = test_gcp_airflow_internal

set -eo pipefail

table=${1}

if [ -z "${table}" ]; then
    echo "$0 is a script that updates the Bigquery tables when the schema changes"
    echo "Usage: $0 <table>"
    exit 1
fi


bq update $PROJECT_ID:$DATASET_ID."${table}" schemas/history_"${table}"_schema.json 