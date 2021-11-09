-- Create a liquidity provider view to track provider positions
WITH total_providers AS 
(
    SELECT op.source_account, 
        op.details.liquidity_pool_id AS liquidity_pool_id, 
        SUM(COALESCE(op.details.shares_received, 0)) AS shares_deposited,
        SUM(COALESCE(op.details.shares, 0)) AS shares_withdrawn,
        SUM(COALESCE(op.details.shares_received, 0)) - SUM(COALESCE(op.details.shares, 0)) AS total_shares
    FROM `PROJECT.DATASET.history_operations` op
    JOIN `PROJECT.DATASET.history_transactions` txn
        ON op.transaction_id = txn.id
    -- Protocol 18 ops are 22 (deposit) and 23 (withdraw)
    WHERE (op.type=22 OR op.type=23)
        AND (txn.successful = true OR txn.successful IS NULL)
    GROUP BY liquidity_pool_id, 
        op.source_account
    )
SELECT A.liquidity_pool_id, 
    B.asset_pair, 
    A.source_account AS provider_account,
    shares_deposited,
    shares_withdrawn,
    total_shares, 
    COUNT(*) count_pool_providers
FROM total_providers A
JOIN `PROJECT.DATASET.v_liquidity_pools_current` B 
    ON A.liquidity_pool_id = B.liquidity_pool_id
-- Filter for active pool providers
WHERE total_shares > 0
GROUP BY A.liquidity_pool_id, 
    B.asset_pair, 
    A.source_account,
    shares_deposited,
    shares_withdrawn,
    total_shares