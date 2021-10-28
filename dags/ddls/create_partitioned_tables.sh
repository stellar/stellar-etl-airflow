# /bin/bash -e 
#
#
###############################################################################
# Ad Hoc script to update existing Bigquery tables for schema changes
#
# Author: Sydney Wiseman
# Date: 1 October 2021
#
# Script will read an updated schema from the /schemas directory
# and create history archives tables. The largest tables are partitioned by 
# batch execution date, which is a proxy for ledger closed date. The smaller
# tables do not need to be partitioned. This script is intended for test env
# use when setting up a sandbox.
###############################################################################
 
cd ../../
WORKDIR="$(pwd)"
echo "Current working directory: $WORKDIR"

PROJECT_ID=test-hubble-261722
DATASET_ID=crypto_stellar_internal_2
SCHEMA_DIR=$WORKDIR/schemas/
PARTITION_TABLES=(history_operations history_transactions history_ledgers)
TABLES=(history_assets history_trades)


# make partitioned tables
for table in ${PARTITION_TABLES[@]}
do 
    echo "Creating partitioned table $table in $DATASET_ID"
    bq mk --table \
    --schema $SCHEMA_DIR${table}_schema.json \
    --time_partitioning_field batch_run_date \
    --time_partitioning_type MONTH \
    $PROJECT_ID:$DATASET_ID.$table
done

#make non-partitioned tables
for table in ${TABLES[@]}
do 
    echo "Creating table $table in $DATASET_ID"
    bq mk --table \
    --schema $SCHEMA_DIR/${table}_schema.json \
    $PROJECT_ID:$DATASET_ID.$table
done
