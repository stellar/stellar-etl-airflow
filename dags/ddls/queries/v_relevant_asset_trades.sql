-- View reports trading for relevant assets only
-- Both AMM and DEX trades are found in the view
-- and both the buy and sell side of the trade must
-- be considered relevant to appear
with
    find_relevant_sales as (
        select *
        from `PROJECT.DATASET.history_trades` as t
        left outer join `PROJECT.crypto_stellar_internal.meaningful_assets` as a
            on t.selling_asset_code = a.code
            and t.selling_asset_issuer = a.issuer
        where
            a.code is not null
            or t.selling_asset_type = 'native'
    )
    , find_relevant_buys as (
        select
            history_operation_id
            , `order`
        from `PROJECT.DATASET.history_trades` as t
        left outer join `PROJECT.crypto_stellar_internal.meaningful_assets` as a
            on t.buying_asset_code = a.code
            and t.buying_asset_issuer = a.issuer
        where
            a.code is not null
            or t.buying_asset_type = 'native'
    )
select
    s.ledger_closed_at
    , s.selling_account_address
    , s.selling_asset_code
    , s.selling_asset_issuer
    , s.selling_asset_type
    , s.selling_amount
    , s.buying_account_address
    , s.buying_asset_code
    , s.buying_asset_issuer
    , s.buying_asset_type
    , s.buying_amount
    , s.price_n
    , s.price_d
    , s.selling_liquidity_pool_id
    , s.liquidity_pool_fee
    , case when selling_liquidity_pool_id is not null then 'AMM' else 'DEX' end as trade_type
from find_relevant_sales as s
join find_relevant_buys as b
    on s.history_operation_id = b.history_operation_id
    and s.order = b.order
