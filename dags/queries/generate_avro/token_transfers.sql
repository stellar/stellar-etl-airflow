export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        transaction_hash
        , transaction_id
        , operation_id
        , contract_id
        , closed_at
        , ledger_sequence
        , `to`
        , `from`
        , asset
        , asset_type
        , asset_code
        , asset_issuer
        , amount_raw
        -- Removing amount because certain assets don't use the 10e-7 precision
        -- , amount
        , event_topic
        , event_type
        , is_soroban
        , unique_key
        , to_muxed
        , to_muxed_id
    from {project_id}.{dataset_id}.token_transfers
    where
        true
        and closed_at >= timestamp('{batch_run_date}')
        and closed_at < timestamp('{next_batch_run_date}')
    order by closed_at asc
)
