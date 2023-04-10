-- Finds the latest state of each claimable balance in the `claimable_balances` table.
-- Ranks each record (grain: one row per balance id) using
-- last modified ledger sequence number. View includes all claimable balances.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
with
    current_balances as (
        select
            b.balance_id
            , b.asset_type
            , b.asset_code
            , b.asset_issuer
            , b.asset_amount
            , b.sponsor
            , b.flags
            , b.last_modified_ledger
            , b.ledger_entry_change
            , l.closed_at
            , b.deleted
            , dense_rank() over (
                partition by b.balance_id 
                order by b.last_modified_ledger desc, b.ledger_entry_change desc
                ) as rank_number
        from `hubble-261722.crypto_stellar_internal_2.claimable_balances` as b
        join `hubble-261722.crypto_stellar_internal_2.history_ledgers` as l
            on b.last_modified_ledger = l.sequence
        group by
            balance_id
            , asset_type
            , asset_code
            , asset_issuer
            , asset_amount
            , sponsor
            , flags
            , last_modified_ledger
            , ledger_entry_change
            , closed_at
            , deleted
    )
select
    balance_id
    , asset_type
    , asset_code
    , asset_issuer
    , asset_amount
    , sponsor
    , flags
    , last_modified_ledger
    , ledger_entry_change
    , closed_at
    , deleted
from current_balances
where rank_number = 1
