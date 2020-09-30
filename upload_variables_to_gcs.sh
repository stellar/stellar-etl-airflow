#!/bin/bash

set -eo pipefail

variables_file=${1}
cloud_composer_environment=${2}
cloud_composer_location=${3}

if [ -z "${variables_file}" ] || [ -z "${cloud_composer_environment}" ] || [ -z "${cloud_composer_location}" ]; then
    echo "$0 is a script that imports the Airflow variables from the provided file"
    echo "Usage: $0 <variables_file> <cloud_composer_environment> <cloud_composer_location>"
    exit 1
fi
if [ -f "${variables_file}" ]; then
    gcloud composer environments storage data import \
    --environment "${cloud_composer_environment}" --location "${cloud_composer_location}" \
    --source "${variables_file}"

    gcloud composer environments run \
    "${cloud_composer_environment}" --location "${cloud_composer_location}" \
    variables -- --import /home/airflow/gcs/data/"${variables_file}"
else echo "$1 must be a file"
fi
