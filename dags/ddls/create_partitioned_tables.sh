#! /bin/bash -e 
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

PROJECT_ID=crypto-stellar
DATASET_ID=crypto_stellar_2
SCHEMA_DIR=$WORKDIR/schemas/
PARTITION_TABLES=(history_operations history_transactions history_ledgers history_assets history_tradesaccounts claimable_balances offers liquidity_pools account_signers trust_lines)

# make partitioned tables
for table in ${PARTITION_TABLES[@]}
do 
    echo "Creating partitioned table $table in $DATASET_ID"
    if [ "$table" = "history_operations" ]; then 
        cluster=id,transaction_id,source_account
    elif [ "$table" = "history_transactions" ]; then
        cluster=id,ledger_sequence,account
    elif [ "$table" = "history_ledgers" ]; then
        cluster=sequence,closed_at
    elif [ "$table" = "history_assets" ]; then
        cluster=asset_code,asset_type,asset_issuer
    elif [ "$table" = "accounts" ]; then
        cluster=account_id,last_modified_ledger
    elif [ "$table" = "claimable_balances" ]; then
        cluster=balance_id,last_modified_ledger,sponsor
    elif [ "$table" = "offers" ]; then
        cluster=seller_id,last_modified_ledger
    elif [ "$table" = "liquidity_pools" ]; then
        cluster=liquidity_pool_id,last_modified_ledger
    elif [ "$table" = "account_signers" ]; then
        cluster=account_id,last_modified_ledger
    elif [ "$table" = "trust_lines" ]; then
        cluster=account_id,last_modified_ledger
    else 
        cluster=history_operation_id,ledger_closed_at
    fi 
    bq mk --table \
    --schema $SCHEMA_DIR${table}_schema.json \
    --time_partitioning_field batch_run_date \
    --time_partitioning_type MONTH \
    --clustering_fields $cluster \
    $PROJECT_ID:$DATASET_ID.$table
done

