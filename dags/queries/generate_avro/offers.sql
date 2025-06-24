export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        seller_id
        , offer_id
        , selling_asset_type
        , selling_asset_code
        , selling_asset_issuer
        , selling_asset_id
        , buying_asset_type
        , buying_asset_code
        , buying_asset_issuer
        , buying_asset_id
        , amount
        , pricen
        , priced
        , price
        , flags
        , last_modified_ledger
        , ledger_entry_change
        , deleted
        , sponsor
        , closed_at
        , ledger_sequence
    from {project_id}.{dataset_id}.offers
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
