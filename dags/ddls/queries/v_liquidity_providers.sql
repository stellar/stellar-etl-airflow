-- Create a liquidity provider view to track provider positions
with
    total_providers as (
        select
            op.source_account
            , op.details.liquidity_pool_id as liquidity_pool_id
            , min(l.closed_at) as first_deposit_date
            , sum(coalesce(op.details.shares_received, 0)) as shares_deposited
            , sum(coalesce(op.details.shares, 0)) as shares_withdrawn
            , sum(coalesce(op.details.shares_received, 0)) - sum(coalesce(op.details.shares, 0)) as total_shares
        from `PROJECT.DATASET.history_operations` as op
        join `PROJECT.DATASET.history_transactions` as txn
            on op.transaction_id = txn.id
        join `PROJECT.DATASET.history_ledgers` as l
            on txn.ledger_sequence = l.sequence
        -- Protocol 18 ops are 22 (deposit) and 23 (withdraw)
        where
            (op.type = 22 or op.type = 23)
            and (txn.successful = true or txn.successful is null)
        group by
            liquidity_pool_id
            , op.source_account
    )
select
    a.liquidity_pool_id
    , b.asset_pair
    , a.source_account as provider_account
    , first_deposit_date
    , shares_deposited
    , shares_withdrawn
    , total_shares
    , count(*) as count_pool_providers
from total_providers as a
join `PROJECT.DATASET.v_liquidity_pools_current` as b
    on a.liquidity_pool_id = b.liquidity_pool_id
group by
    a.liquidity_pool_id
    , b.asset_pair
    , a.source_account
    , first_deposit_date
    , shares_deposited
    , shares_withdrawn
    , total_shares
