export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          liquidity_pool_id
        , `type`
        , fee
        , trustline_count
        , pool_share_count
        , asset_a_type
        , asset_a_code
        , asset_a_issuer
        , asset_a_id
        , asset_a_amount
        , asset_b_type
        , asset_b_code
        , asset_b_issuer
        , asset_b_id
        , asset_b_amount
        , last_modified_ledger
        , ledger_entry_change
        , deleted
        , closed_at
        , ledger_sequence
        , liquidity_pool_id_strkey
    from {project_id}.{dataset_id}.liquidity_pools
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
