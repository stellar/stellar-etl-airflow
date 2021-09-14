# /bin/bash -e 
 
cd ../../
WORKDIR="$(pwd)"
echo "Current working directory: $WORKDIR"

PROJECT_ID=test-hubble-319619
DATASET_ID=test_gcp_airflow_internal_partitioned
SCHEMA_DIR=$WORKDIR/schemas/
PARTITION_TABLES=(history_operations history_transactions history_ledgers)
TABLES=(history_assets history_trades)


# make partitioned tables
for table in ${PARTITION_TABLES[@]}
do 
    echo "Creating partitioned table $table in $DATASET_ID"
    bq mk --table \
    --schema $SCHEMA_DIR$table.json \
    --time_partitioning_field batch_run_date \
    --time_partitioning_type MONTH \
    $PROJECT_ID:$DATASET_ID.$table
done

#make non-partitioned tables
# for table in ${TABLES[@]}
# do 
#     echo "Creating table $table in $DATASET_ID"
#     bq mk --table \
#     --schema $SCHEMA_DIR/$table.json \
#     $PROJECT_ID:$DATASET_ID.$table
# done
