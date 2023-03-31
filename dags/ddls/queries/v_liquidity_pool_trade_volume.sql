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
            , t.selling_amount
            , t.buying_account_address
            , t.buying_asset_code
            , t.buying_amount
            , c.asset_a_code
            , c.asset_a_issuer
            , c.asset_b_code
            , c.asset_b_issuer
            , c.asset_pair
            , t.price_n
            , t.price_d
            , case
                when t.selling_asset_code = c.asset_b_code then t.selling_amount else t.buying_amount
            end as trade_amount
            , 1 + (t.liquidity_pool_fee / 10000) as fee_multiplier
        from `PROJECT.DATASET.history_trades` as t
        join `PROJECT.DATASET.v_liquidity_pools_current` as c
            on t.selling_liquidity_pool_id = c.liquidity_pool_id
    )
select
    ledger_closed_at
    , selling_liquidity_pool_id
    , asset_a_code
    , asset_a_issuer
    , asset_b_code
    , asset_b_issuer
    , asset_pair
    , selling_asset_code
    , selling_amount
    , buying_asset_code
    , buying_amount
    , trade_amount - (trade_amount / fee_multiplier) as fee_earned
    , trade_amount / fee_multiplier as trade_amount
    , price_n
    , price_d
    , case when selling_asset_code = asset_b_code then price_d / price_n else price_n / price_d end as quote_price
    , case when selling_asset_code = asset_b_code then price_n / price_d else price_d / price_n end as base_price
from trade_volume
