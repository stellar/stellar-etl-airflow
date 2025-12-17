export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        day
      , account_id
      , asset_type
      , asset_code
      , asset_issuer
      , contract_id
      , liquidity_pool_balance
      , offer_balance
      , trustline_balance
      , contract_balance
    from {project_id}.{dataset_id}.account_balances__daily_agg
    where
        true
        and day >= date_trunc(date('{batch_run_date}'), day)
        and day < date_trunc(date('{next_batch_run_date}', day)
    order by day asc
)
