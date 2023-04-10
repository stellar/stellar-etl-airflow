-- Finds the trade volume of liquidity pools only (No DEX trades)
-- Prices are stated in base and quote price to make price tracking
-- easy. Fees are also subtracted from the trade amount and stated as its
-- own column so that fees and APR can be evaluated.
with
    trade_volume as (
        select
            t.ledger_closed_at
            , t.selling_liquidity_pool_id
            , t.selling_asset_code
            , t.selling_asset_issuer
            , t.selling_asset_type
            , t.selling_amount
            , t.buying_account_address
            , t.buying_asset_code
            , t.buying_asset_issuer
            , t.buying_asset_type
            , t.buying_amount
            , c.asset_a_code
            , c.asset_a_issuer
            , c.asset_b_code
            , c.asset_b_issuer
            , c.asset_pair
            , t.price_n
            , t.price_d
            , 1 + (t.liquidity_pool_fee / 10000) as fee_multiplier
        from `PROJECT.DATASET.history_trades` as t
        join `PROJECT.DATASET.v_liquidity_pools_current` as c
            on t.selling_liquidity_pool_id = c.liquidity_pool_id
    )
    , asset_price as (
        select
            asset_code
            , asset_issuer
            , price_in_xlm as price
            , last_updated_ts as valid_from_ts
            , lead(last_updated_ts, 1, '9999-12-31') over (
                partition by asset_code, asset_issuer
                order by last_updated_ts asc
            ) as valid_to_ts
        from `PROJECT.DATASET.asset_prices_xlm`
    )
    , xlm_price as (
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
    t.ledger_closed_at
    , t.selling_liquidity_pool_id
    , t.asset_a_code
    , t.asset_a_issuer
    , t.asset_b_code
    , t.asset_b_issuer
    , t.asset_pair
    , t.selling_asset_code
    , t.selling_amount
    , t.buying_asset_code
    , t.buying_amount
    , p.price as asset_price_in_xlm
    , x.price as xlm_price
    , case
        when t.buying_asset_type = 'native'
            then
                (t.buying_amount * coalesce(x.price, 0))
                - (t.buying_amount * coalesce(x.price, 0)) / t.fee_multiplier
        else
            (t.buying_amount * coalesce(p.price, 0) * coalesce(x.price, 0))
            - (t.buying_amount * coalesce(p.price, 0) * coalesce(x.price, 0)) / t.fee_multiplier
    end as fee_earned
    , case
        when t.buying_asset_type = 'native'
            then (t.buying_amount * coalesce(x.price, 0)) / t.fee_multiplier
        else (t.buying_amount * coalesce(p.price, 0) * coalesce(x.price, 0)) / t.fee_multiplier
    end as trade_amount
    , price_n
    , price_d
    , case when selling_asset_code = asset_b_code then price_d / price_n else price_n / price_d end as quote_price
    , case when selling_asset_code = asset_b_code then price_n / price_d else price_d / price_n end as base_price
from trade_volume as t
left outer join asset_price as p
    on t.buying_asset_code = p.asset_code
    and t.buying_asset_issuer = p.asset_issuer
    and t.ledger_closed_at >= p.valid_from_ts
    and t.ledger_closed_at < p.valid_to_ts
left outer join xlm_price as x
    on t.ledger_closed_at >= x.valid_from_ts
    and t.ledger_closed_at < x.valid_to_ts