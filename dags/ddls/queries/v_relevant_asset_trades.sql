-- View reports trading for relevant assets only
-- Both AMM and DEX trades are found in the view
-- and both the buy and sell side of the trade must
-- be considered relevant to appear 
WITH find_relevant_sales AS 
(
    SELECT *
    FROM `PROJECT.DATASET.history_trades` t
    LEFT OUTER JOIN `PROJECT.crypto_stellar_internal.meaningful_assets` a
        ON t.selling_asset_code = a.code
        AND t.selling_asset_issuer = a.issuer
        WHERE a.code IS NOT NULL
            OR t.selling_asset_type = 'native'
    ),
find_relevant_buys AS 
(
    SELECT history_operation_id, `order`
    FROM `PROJECT.DATASET.history_trades` t
    LEFT OUTER JOIN `PROJECT.crypto_stellar_internal.meaningful_assets` a
    ON t.buying_asset_code = a.code
    AND t.buying_asset_issuer = a.issuer
    WHERE a.code IS NOT NULL 
        OR t.buying_asset_type = 'native'
    )
SELECT S.ledger_closed_at, 
    S.selling_account_address, 
    S.selling_asset_code, 
    S.selling_asset_issuer, 
    S.selling_asset_type,
    S.selling_amount, 
    S.buying_account_address, 
    S.buying_asset_code, 
    S.buying_asset_issuer, 
    S.buying_asset_type,
    S.buying_amount, 
    S.price_n, 
    S.price_d, 
    S.selling_liquidity_pool_id, 
    S.liquidity_pool_fee,
CASE WHEN selling_liquidity_pool_id IS NOT NULL THEN 'AMM' ELSE 'DEX' END AS trade_type
FROM find_relevant_sales S 
JOIN find_relevant_buys B
    ON S.history_operation_id = B.history_operation_id
    AND S.order = B.order