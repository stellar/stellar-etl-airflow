-- Finds the latest state of each claimable balance in the `claimable_balances` table.
-- Ranks each record (grain: one row per balance id) using 
-- last modified ledger sequence number. View includes all claimable balances.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
WITH current_balances AS 
(
    SELECT B.balance_id,
        B.asset_type,
        B.asset_code,
        B.asset_issuer,
        B.asset_amount,
        B.sponsor,
        B.flags,
        B.last_modified_ledger,
        B.ledger_entry_change,
        L.closed_at,
        B.deleted,
        DENSE_RANK() OVER(PARTITION BY B.balance_id ORDER BY B.last_modified_ledger DESC) AS rank_number
    FROM `hubble-261722.crypto_stellar_internal_2.claimable_balances` B
    JOIN `hubble-261722.crypto_stellar_internal_2.history_ledgers` L
        ON B.last_modified_ledger = L.sequence
    GROUP BY balance_id,
        asset_type,
        asset_code,
        asset_issuer,
        asset_amount,
        sponsor,
        flags,
        last_modified_ledger, 
        ledger_entry_change,
        closed_at,
        deleted
    )
SELECT balance_id,
    asset_type,
    asset_code,
    asset_issuer,
    asset_amount,
    sponsor,
    flags,
    last_modified_ledger, 
    ledger_entry_change,
    closed_at,
    deleted
FROM current_balances  
WHERE rank_number = 1