export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          history_operation_id
        , `order`
        , selling_account_address
        , selling_asset_code
        , selling_asset_issuer
        , selling_asset_type
        , selling_asset_id
        , selling_amount
        , buying_account_address
        , buying_asset_code
        , buying_asset_issuer
        , buying_asset_type
        , buying_asset_id
        , buying_amount
        , price_n
        , price_d
        , selling_offer_id
        , buying_offer_id
        , selling_liquidity_pool_id
        , liquidity_pool_fee
        , trade_type
        , rounding_slippage
        , seller_is_exact
        , selling_liquidity_pool_id_strkey
        , ledger_closed_at as closed_at
    from {project_id}.{dataset_id}.history_trades
    where
        true
        and ledger_closed_at >= '{batch_run_date}'
        and ledger_closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
