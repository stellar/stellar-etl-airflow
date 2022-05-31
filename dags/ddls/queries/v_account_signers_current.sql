-- Finds the latest state of each account signer in the `account_signers` table.
-- Ranks each record (grain: one row per account) using 
-- last modified ledger sequence number. View includes all account signers.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
WITH current_signers AS 
(
    SELECT S.account_id,
        S.signer,
        S.weight,
        S.sponsor,
        S.last_modified_ledger,
        L.closed_at,
        S.ledger_entry_change,
        S.deleted,
        DENSE_RANK() OVER(PARTITION BY S.account_id, S.signer ORDER BY S.last_modified_ledger DESC) AS rank_number
    FROM `hubble-261722.crypto_stellar_internal_2.account_signers` S
    JOIN `hubble-261722.crypto_stellar_internal_2.history_ledgers` L
        ON S.last_modified_ledger = L.sequence
    GROUP BY account_id,
        signer,
        weight,
        sponsor,
        last_modified_ledger, 
        ledger_entry_change,
        closed_at,
        deleted
    )
SELECT account_id, 
    signer,
    weight,
    sponsor
    last_modified_ledger, 
    ledger_entry_change,
    closed_at,
    deleted
FROM current_signers   
WHERE rank_number = 1