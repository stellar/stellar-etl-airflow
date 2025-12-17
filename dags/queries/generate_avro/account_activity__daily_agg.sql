export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        day
        , address
        , asset_code
        , asset_issuer
        , asset_type
        , contract_id
        , direct_payment_volume
        , path_payment_volume
        , payment_arbitrage_volume
        , payment_volume
        , orderbook_trade_volume
        , amm_trade_volume
        , total_dex_trade_volume
        , liquidity_pool_deposit_volume
        , liquidity_pool_withdraw_volume
        , smart_contract_volume
    from {project_id}.{dataset_id}.account_activity__daily_agg
    where
        true
        and day >= date_trunc(date('{batch_run_date}'), day)
        and day < date_trunc(date('{next_batch_run_date}'), day)
    order by day asc
)
