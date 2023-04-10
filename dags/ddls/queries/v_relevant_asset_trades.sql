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
    ),
    asset_price as (
        select 
            asset_code
            , asset_issuer, price_in_xlm as price
            , last_updated_ts as valid_from_ts
            , lead(last_updated_ts, 1, '9999-12-31') over(
                partition by asset_code, asset_issuer 
                order by last_updated_ts asc
            ) as valid_to_ts
        from `hubble-261722.crypto_stellar_internal_2.asset_prices_xlm`
    ),
    xlm_price as (
        select 
            '' as asset_code
            , '' as asset_issuer
            , price_in_usd as price
            , last_updated_ts as valid_from_ts
            , lead(last_updated_ts, 1, '9999-12-31') over (
                partition by asset_id 
                order by last_updated_ts asc
            ) as valid_to_ts
        from `hubble-261722.crypto_stellar_internal_2.asset_prices_usd`
    )
select
    s.ledger_closed_at
    , s.selling_account_address
    , s.selling_asset_code
    , s.selling_asset_issuer
    , s.selling_asset_type
    , s.selling_amount
    , case when s.selling_asset_type = 'native' 
        then s.selling_amount * coalesce(x.price, 0) 
        else s.selling_amount * coalesce(x.price, 0) * coalesce(c.price, 0)
        end as selling_amount_usd,
    , s.buying_account_address
    , s.buying_asset_code
    , s.buying_asset_issuer
    , s.buying_asset_type
    , s.buying_amount
    , case when s.buying_asset_type = 'native' 
        then s.buying_amount * coalesce(x.price, 0) 
        else s.buying_amount * COALESCE(x.price, 0) * COALESCE(d.price, 0)
        end as buying_amount_usd,
    , s.price_n
    , s.price_d
    , s.selling_liquidity_pool_id
    , s.liquidity_pool_fee
    , case when selling_liquidity_pool_id is not null then 'AMM' else 'DEX' end as trade_type
from find_relevant_sales as s
join find_relevant_buys as b
    on s.history_operation_id = b.history_operation_id
    and s.order = b.order
left outer join asset_price c
    on s.selling_asset_code = c.asset_code
    and s.selling_asset_issuer = c.asset_issuer
    and s.ledger_closed_at >= c.valid_from_ts
    and s.ledger_closed_at < c.valid_to_ts
left outer join asset_price d
    on s.buying_asset_code = d.asset_code
    and s.buying_asset_issuer = d.asset_issuer
    and s.ledger_closed_at >= d.valid_from_ts
    and s.ledger_closed_at < d.valid_to_ts
left outer join xlm_price x
    on s.ledger_closed_at >= x.valid_from_ts
    and s.ledger_closed_at < x.valid_to_ts  
