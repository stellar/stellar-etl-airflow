# /bin/bash -e 
 
cd ../../
WORKDIR="$(pwd)"
echo "Current working directory: $WORKDIR"

PROJECT_ID=hubble-261722
DATASET_ID=crypto_stellar_internal_2
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
