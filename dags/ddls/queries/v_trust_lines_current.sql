-- Finds the latest state of each trustline in the `trust_lines` table.
-- Ranks each record (grain: trust line per account per asset) using 
-- last modified ledger sequence number. View includes all trust lines.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
WITH current_tls AS 
(
    SELECT TL.account_id,
        TL.asset_code, 
        TL.asset_issuer, 
        TL.asset_type,
        TL.liquidity_pool_id, 
        TL.balance, 
        TL.buying_liabilities,
        TL.selling_liabilities, 
        TL.flags, 
        TL.sponsor,
        TL.trust_line_limit,
        TL.last_modified_ledger,
        L.closed_at,
        TL.deleted,
        DENSE_RANK() OVER(PARTITION BY TL.account_id, TL.asset_code, TL.asset_issuer, TL.liquidity_pool_id ORDER BY TL.last_modified_ledger DESC) AS rank_number
    FROM `PROJECT.DATASET.trust_lines` TL
    JOIN `PROJECT.DATASET.history_ledgers` L
        ON TL.last_modified_ledger = L.sequence
    GROUP BY account_id, 
        asset_code, 
        asset_issuer, 
        asset_type,
        liquidity_pool_id, 
        balance, 
        buying_liabilities,
        selling_liabilities, 
        flags, 
        sponsor, 
        trust_line_limit,
        last_modified_ledger, 
        closed_at, 
        deleted
    )
SELECT account_id, 
    asset_code, 
    asset_issuer, 
    asset_type,
    liquidity_pool_id, 
    balance, 
    buying_liabilities, 
    selling_liabilities,
    flags, 
    sponsor, 
    trust_line_limit, 
    last_modified_ledger, 
    closed_at,
    deleted
FROM current_tls  
WHERE rank_number = 1