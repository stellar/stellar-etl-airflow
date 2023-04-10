-- Finds the latest state of each account signer in the `account_signers` table.
-- Ranks each record (grain: one row per account) using
-- last modified ledger sequence number. View includes all account signers.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
with
    current_signers as (
        select
            s.account_id
            , s.signer
            , s.weight
            , s.sponsor
            , s.last_modified_ledger
            , l.closed_at
            , s.ledger_entry_change
            , s.deleted
            , dense_rank() over (
                partition by s.account_id, s.signer 
                order by s.last_modified_ledger desc, s.ledger_entry_change desc
            ) as rank_number
        from `hubble-261722.crypto_stellar_internal_2.account_signers` as s
        join `hubble-261722.crypto_stellar_internal_2.history_ledgers` as l
            on s.last_modified_ledger = l.sequence
        group by
            account_id
            , signer
            , weight
            , sponsor
            , last_modified_ledger
            , ledger_entry_change
            , closed_at
            , deleted
    )
select
    account_id
    , signer
    , weight
    , sponsor as last_modified_ledger
    , ledger_entry_change
    , closed_at
    , deleted
from current_signers
where rank_number = 1
