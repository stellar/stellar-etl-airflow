-- Finds the latest state of each trustline in the `trust_lines` table.
-- Ranks each record (grain: trust line per account per asset) using
-- last modified ledger sequence number. View includes all trust lines.
-- (Deleted and Existing). View matches the Horizon snapshotted state tables.
with
    current_tls as (
        select
            tl.account_id
            , tl.asset_code
            , tl.asset_issuer
            , tl.asset_type
            , tl.liquidity_pool_id
            , tl.balance
            , tl.buying_liabilities
            , tl.selling_liabilities
            , tl.flags
            , tl.sponsor
            , tl.trust_line_limit
            , tl.last_modified_ledger
            , l.closed_at
            , tl.deleted
            , dense_rank() over (
                partition by tl.account_id, tl.asset_code, tl.asset_issuer, tl.liquidity_pool_id
                order by tl.last_modified_ledger desc
            ) as rank_number
        from `PROJECT.DATASET.trust_lines` as tl
        join `PROJECT.DATASET.history_ledgers` as l
            on tl.last_modified_ledger = l.sequence
        group by
            account_id
            , asset_code
            , asset_issuer
            , asset_type
            , liquidity_pool_id
            , balance
            , buying_liabilities
            , selling_liabilities
            , flags
            , sponsor
            , trust_line_limit
            , last_modified_ledger
            , closed_at
            , deleted
    )
select
    account_id
    , asset_code
    , asset_issuer
    , asset_type
    , liquidity_pool_id
    , balance
    , buying_liabilities
    , selling_liabilities
    , flags
    , sponsor
    , trust_line_limit
    , last_modified_ledger
    , closed_at
    , deleted
from current_tls
where rank_number = 1
