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
PARTITION_TABLES=(history_operations history_transactions history_ledgers history_assets history_trades history_effects accounts claimable_balances offers liquidity_pools account_signers trust_lines)

# make partitioned tables
for table in ${PARTITION_TABLES[@]}; do
    echo "Creating partitioned table $table in $DATASET_ID"
    if [ "$table" = "history_operations" ]; then
        cluster=transaction_id,source_account,type
        partition=batch_run_date
    elif [ "$table" = "history_transactions" ]; then
        cluster=account,ledger_sequence,successful
        partition=batch_run_date
    elif [ "$table" = "history_ledgers" ]; then
        cluster=sequence,closed_at
        partition=closed_at
    elif [ "$table" = "history_assets" ]; then
        cluster=asset_code,asset_issuer,asset_type
        partition=batch_run_date
    elif [ "$table" = "history_trades" ]; then
        cluster=selling_asset_id,buying_asset_id,trade_type
        partition=ledger_closed_at
    elif [ "$table" = "account" ]; then
        cluster=account_id,last_modified_ledger
        partition=batch_run_date
    elif [ "$table" = "claimable_balances" ]; then
        cluster=asset_id,last_modified_ledger
        partition=batch_run_date
    elif [ "$table" = "offers" ]; then
        cluster=selling_asset_id,buying_asset_id,last_modified_ledger
        partition=batch_run_date
    elif [ "$table" = "liquidity_pools" ]; then
        cluster=liquidity_pool_id,asset_a_id,asset_b_id,last_modified_ledger
        partition=batch_run_date
    elif [ "$table" = "account_signers" ]; then
        cluster=account_id,signer,last_modified_ledger
        partition=batch_run_date
    elif [ "$table" = "trust_lines" ]; then
        cluster=account_id,asset_id,liquidity_pool_id,last_modified_ledger
        partition=batch_run_date
    else
        cluster=ledger_sequence,transaction_id,accounttype
        partition=closed_at
    fi
    bq mk --table \
        --schema $SCHEMA_DIR${table}_schema.json \
        --time_partitioning_field $partition \
        --time_partitioning_type MONTH \
        --clustering_fields $cluster \
        $PROJECT_ID:$DATASET_ID.$table
done
