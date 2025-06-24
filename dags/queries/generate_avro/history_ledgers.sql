export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          sequence
        , ledger_hash
        , previous_ledger_hash
        , transaction_count
        , operation_count
        , closed_at
        , id
        , total_coins
        , fee_pool
        , base_fee
        , base_reserve
        , max_tx_set_size
        , protocol_version
        , ledger_header
        , successful_transaction_count
        , failed_transaction_count
        , tx_set_operation_count
        , soroban_fee_write_1kb
        , node_id
        , signature
        , total_byte_size_of_bucket_list
    from {project_id}.{dataset_id}.history_ledgers
    where
        true
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
