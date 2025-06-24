export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          ledger_key
        , account_id
        , asset_type
        , asset_issuer
        , asset_code
        , asset_id
        , liquidity_pool_id
        , balance
        , trust_line_limit
        , buying_liabilities
        , selling_liabilities
        , flags
        , last_modified_ledger
        , ledger_entry_change
        , deleted
        , sponsor
        , closed_at
        , ledger_sequence
        , liquidity_pool_id_strkey
    from {project_id}.{dataset_id}.trust_lines
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
