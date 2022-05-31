-- Finds the latest state of each pool in the `liquidity_pool` table.
-- Ranks each record (grain: pool id) using last modified ledger sequence 
-- number. View includes all liquidity pools. (Deleted and Existing)
-- View matches the Horizon snapshotted state tables.
WITH current_lps AS 
(
    SELECT LP.liquidity_pool_id, 
        LP.fee,
        LP.trustline_count,
        LP.pool_share_count,
        -- XLM asset codes are null. Transform to XLM for readibility
        CASE WHEN LP.asset_a_type = 'native' THEN CONCAT('XLM:',LP.asset_b_code)  
            ELSE CONCAT(LP.asset_a_code,':',LP.asset_b_code) END AS asset_pair,
        LP.asset_a_code, 
        LP.asset_a_issuer,
        LP.asset_b_code, 
        LP.asset_b_issuer,
        LP.asset_a_amount, 
        LP.asset_b_amount,
        LP.last_modified_ledger,
        L.closed_at,
        LP.deleted,
        DENSE_RANK() OVER(PARTITION BY liquidity_pool_id ORDER BY LP.last_modified_ledger DESC LP.batch_insert_ts DESC) AS rank_number
    FROM `PROJECT.DATASET.liquidity_pools` LP
    JOIN `PROJECT.DATASET.history_ledgers` L
        ON LP.last_modified_ledger = L.sequence
    )
SELECT liquidity_pool_id, 
    fee, 
    trustline_count, 
    pool_share_count,
    asset_pair, 
    asset_a_code,
    asset_a_issuer,
    asset_b_code, 
    asset_b_issuer, 
    asset_a_amount, 
    asset_b_amount,
    last_modified_ledger, 
    closed_at,
    deleted
FROM current_lps 
WHERE rank_number = 1
group by liquidity_pool_id, 
    fee, 
    trustline_count, 
    pool_share_count,
    asset_pair, 
    asset_a_code,
    asset_a_issuer,
    asset_a_type,
    asset_b_code, 
    asset_b_issuer, 
    asset_b_type,
    asset_a_amount, 
    asset_b_amount,
    last_modified_ledger, 
    closed_at,
    deleted
    