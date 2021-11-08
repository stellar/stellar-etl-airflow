-- Finds the trade volume of liquidity pools only (No DEX trades)
-- Prices are stated in base and quote price to make price tracking
-- easy. Fees are also subtracted from the trade amount and stated as its
-- own column so that fees and APR can be evaluated.
WITH trade_volume AS (
    SELECT T.ledger_closed_at, 
        T.selling_liquidity_pool_id, 
        T.selling_asset_code, 
        T.selling_amount, 
        T.buying_account_address, 
        T.buying_asset_code, 
        T.buying_amount,
        C.asset_a_code, 
        C.asset_a_issuer, 
        C.asset_b_code, 
        C.asset_b_issuer,
        C.asset_pair,
        T.price_n, T.price_d,
        CASE WHEN T.selling_asset_code = C.asset_b_code THEN T.selling_amount ELSE T.buying_amount END AS trade_amount,
        1 + (T.liquidity_pool_fee / 10000) AS fee_multiplier
    FROM `PROJECT.DATASET.history_trades` T
    JOIN `PROJECT.DATASET.v_liquidity_pools_current` C
    ON T.selling_liquidity_pool_id = C.liquidity_pool_id
    )
SELECT ledger_closed_at, 
    selling_liquidity_pool_id, 
    asset_a_code, 
    asset_a_issuer, 
    asset_b_code, 
    asset_b_issuer,
    asset_pair,
    selling_asset_code, 
    selling_amount, 
    buying_asset_code, 
    buying_amount,
    trade_amount - (trade_amount / fee_multiplier) AS fee_earned,
    trade_amount / fee_multiplier AS trade_amount, 
    price_n, 
    price_d,    
    CASE WHEN selling_asset_code = asset_b_code THEN price_d / price_n ELSE price_n / price_d END AS quote_price,
    CASE WHEN selling_asset_code = asset_b_code THEN price_n / price_d ELSE price_d / price_n END AS base_price
FROM trade_volume
