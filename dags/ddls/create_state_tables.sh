# /bin/bash -e 
#
#
###############################################################################
# Ad Hoc script to create all state tables
#
# Author: Sydney Wiseman
# Date: 1 October 2021
#
# Script will create the state tables in environment specified through 
# project id and dataset id. This script is intended for test environment
# use for easy creation and deletion of tables when developing. The script
# reads schemas from the schemas directory and creates as a table with 
# clustering on the `last_modified_ledger` field
#
# user should replace the dataset id with necessary dataset
###############################################################################
 
cd ../../
WORKDIR="$(pwd)"
echo "Current working directory: $WORKDIR"

PROJECT_ID=test-hubble-319619
DATASET_ID=test_gcp_airflow_internal
SCHEMA_DIR=$WORKDIR/schemas/
STATE_TABLES=(accounts liquidity_pools offers trust_lines)

# make state tables
for table in ${STATE_TABLES[@]}
do 
    echo "Creating state table $table in $DATASET_ID"
    bq mk --table \
    --schema $SCHEMA_DIR${table}_schema.json \
    --clustering_fields last_modified_ledger \
    $PROJECT_ID:$DATASET_ID.$table
done
