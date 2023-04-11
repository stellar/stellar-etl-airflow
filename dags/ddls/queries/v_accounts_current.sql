-- Finds the latest state of each account in the `accounts` table.
-- Ranks each record (grain: one row per account) using
-- last modified ledger sequence number. View includes all accounts.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
with
    current_accts as (
        select
            a.account_id
            , a.balance
            , a.buying_liabilities
            , a.selling_liabilities
            , a.sequence_number
            , a.sequence_ledger
            , a.sequence_time
            , a.num_subentries
            , a.num_sponsoring
            , a.num_sponsored
            , a.inflation_destination
            , a.flags
            , a.home_domain
            , a.master_weight
            , a.threshold_low
            , a.threshold_medium
            , a.threshold_high
            , a.last_modified_ledger
            , a.ledger_entry_change
            , l.closed_at
            , a.deleted
            , sponsor
            , dense_rank() over (
                partition by a.account_id
                order by a.last_modified_ledger desc, a.ledger_entry_change desc
                ) as rank_number
        from `hubble-261722.crypto_stellar_internal_2.accounts` as a
        join `hubble-261722.crypto_stellar_internal_2.history_ledgers` as l
            on a.last_modified_ledger = l.sequence
        group by
            account_id
            , balance
            , buying_liabilities
            , selling_liabilities
            , sequence_number
            , sequence_ledger
            , sequence_time
            , num_subentries
            , num_sponsoring
            , num_sponsored
            , inflation_destination
            , flags
            , selling_liabilities
            , flags
            , home_domain
            , master_weight
            , threshold_low
            , threshold_medium
            , threshold_high
            , last_modified_ledger
            , ledger_entry_change
            , closed_at
            , deleted
            , sponsor
    )
select
    account_id
    , balance
    , buying_liabilities
    , selling_liabilities
    , sequence_number
    , sequence_ledger
    , sequence_time
    , num_subentries
    , num_sponsoring
    , num_sponsored
    , inflation_destination
    , flags
    , home_domain
    , master_weight
    , threshold_low
    , threshold_medium
    , threshold_high
    , last_modified_ledger
    , ledger_entry_change
    , closed_at
    , deleted
    , sponsor
from current_accts
where rank_number = 1
