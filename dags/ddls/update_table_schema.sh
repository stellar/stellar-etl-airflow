# bin/bash -e 
#
#
###############################################################################
# Ad Hoc script to update existing Bigquery tables for schema changes
#
# Author: Sydney Wiseman
# Date: 1 October 2021
#
# Script will read an updated schema from the /schemas directory
# and apply changes to the specified table (passed as an argument).
# The project id and dataset ids are hardcoded; if you need to update
# tables for a different environment, change the parameters.
###############################################################################

PROJECT_ID=hubble-261722
DATASET_ID=crypto_stellar_internal_2

set -eo pipefail

table=${1}

if [ -z "${table}" ]; then
    echo "$0 is a script that updates the Bigquery tables when the schema changes"
    echo "Usage: $0 <table>"
    exit 1
fi


bq update ${PROJECT_ID}:${DATASET_ID}."${table}" schemas/"${table}"_schema.json
