/*
Query denormalizes the history_ledgers, history_transactions and history_operations tables at
the `history_operation_id` level. Table is loaded using an append-only load pattern by batch_id.
Note: as attributes are added to the `details` blob in history_operations, it is recommended
to add relevant data as a new field in the enriched_history_operations table.

Table is heavily used for KPI calculation and Metabase Dashboards.
*/
SELECT
-- expanded operations details fields
details.account, details.amount, details.asset_code, details.asset_issuer, details.asset_type, details.authorize,
CASE WHEN details.balance_id IS NOT NULL THEN details.balance_id ELSE details.claimable_balance_id END as balance_id, 
details.buying_asset_code, details.buying_asset_issuer, details.buying_asset_type, details.from, details.funder,
details.high_threshold, details.home_domain, details.inflation_dest, details.into, details.limit, details.low_threshold,
details.master_key_weight, details.med_threshold, details.name, details.offer_id, details.path,
details.price, details.price_r.d, details.price_r.n, details.selling_asset_code, details.selling_asset_issuer, details.selling_asset_type,
details.set_flags, details.set_flags_s, details.signer_key, details.signer_weight, details.source_amount, details.source_asset_code,
details.source_asset_issuer, details.source_asset_type, details.source_max, details.starting_balance, details.to, details.trustee,
details.trustor, details.trustline_asset, details.value, details.clear_flags, details.clear_flags_s, details.destination_min, details.bump_to,
details.sponsor, details.sponsored_id, details.begin_sponsor, details.authorize_to_maintain_liabilities, details.clawback_enabled,
details.liquidity_pool_id, details.reserve_a_asset_type, details.reserve_a_asset_code, details.reserve_a_asset_issuer,
details.reserve_a_max_amount, details.reserve_a_deposit_amount, details.reserve_b_asset_type, details.reserve_b_asset_code,
details.reserve_b_asset_issuer, details.reserve_b_max_amount, details.reserve_b_deposit_amount, details.min_price,
details.min_price_r, details.max_price, details.max_price_r,
details.shares_received, details.reserve_a_min_amount, details.reserve_b_min_amount, details.shares,
details.reserve_a_withdraw_amount, details.reserve_b_withdraw_amount,
-- operation fields
ho.application_order as op_application_order, ho.id as op_id, source_account as op_source_account,
source_account_muxed as op_source_account_muxed, transaction_id, type,
-- transaction fields
transaction_hash, ledger_sequence, ht.application_order as txn_application_order, ht.account as txn_account,
account_sequence, max_fee, ht.operation_count as txn_operation_count, ht.created_at as txn_created_at,
memo_type, memo, time_bounds, successful, fee_charged, fee_account, new_max_fee, account_muxed, fee_account_muxed,
-- ledger fields
ledger_hash, previous_ledger_hash, transaction_count, hl.operation_count as ledger_operation_count, closed_at,
hl.id as ledger_id, total_coins,
fee_pool, base_fee, base_reserve, max_tx_set_size, protocol_version, successful_transaction_count, failed_transaction_count,
ho.batch_id as batch_id, ho.batch_run_date as batch_run_date, current_timestamp() as batch_insert_ts,
--new protocol 19 fields for transaction preconditions
ht.ledger_bounds as ledger_bounds, ht.min_account_sequence as min_account_sequence, 
ht.min_account_sequence_age as min_account_sequence_age, ht.min_account_sequence_ledger_gap as min_account_sequence_ledger_gap,
ht.extra_signers as extra_signers
FROM `{project_id}.{dataset_id}.history_operations` ho
JOIN `{project_id}.{dataset_id}.history_transactions` ht
    on ho.transaction_id=ht.id
JOIN `{project_id}.{dataset_id}.history_ledgers` hl
    on ht.ledger_sequence=hl.sequence
WHERE ho.batch_id = '{batch_id}'
    AND ho.batch_run_date = '{batch_run_date}'
    AND hl.batch_run_date >= '{prev_batch_run_date}'
    AND hl.batch_run_date < '{next_batch_run_date}'
    AND ht.batch_run_date >= '{prev_batch_run_date}'
    AND ht.batch_run_date < '{next_batch_run_date}'