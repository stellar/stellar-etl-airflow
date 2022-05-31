-- Finds the latest state of each account in the `accounts` table.
-- Ranks each record (grain: one row per account) using 
-- last modified ledger sequence number. View includes all accounts.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
WITH current_accts AS 
(
    SELECT A.account_id,
        A.balance, 
        A.buying_liabilities,
        A.selling_liabilities,
        A.sequence_number,
        A.sequence_ledger,
        A.sequence_time,
        A.num_subentries,
        A.inflation_destination,
        A.flags,
        A.home_domain,
        A.master_weight,
        A.threshold_low,
        A.threshold_medium,
        A.threshold_high,
        A.last_modified_ledger,
        A.ledger_entry_change,
        L.closed_at,
        A.deleted,
        sponsor,
        DENSE_RANK() OVER(PARTITION BY A.account_id ORDER BY A.last_modified_ledger DESC) AS rank_number
    FROM `hubble-261722.crypto_stellar_internal_2.accounts` A
    JOIN `hubble-261722.crypto_stellar_internal_2.history_ledgers` L
        ON A.last_modified_ledger = L.sequence
    GROUP BY account_id, 
        balance,
        buying_liabilities, 
        selling_liabilities, 
        sequence_number,
        sequence_ledger,
        sequence_time,
        num_subentries, 
        inflation_destination, 
        flags, 
        selling_liabilities,
        flags, 
        home_domain, 
        master_weight,
        threshold_low,
        threshold_medium,
        threshold_high, 
        last_modified_ledger, 
        ledger_entry_change,
        closed_at,
        deleted,
        sponsor
    )
SELECT account_id, 
    balance,
    buying_liabilities, 
    selling_liabilities, 
    sequence_number,
    sequence_ledger,
    sequence_time,
    num_subentries, 
    inflation_destination, 
    flags,    
    home_domain, 
    master_weight,
    threshold_low,
    threshold_medium,
    threshold_high, 
    last_modified_ledger, 
    ledger_entry_change,
    closed_at,
    deleted,
    sponsor
FROM current_accts   
WHERE rank_number = 1