SELECT
-- expanded operations details fields
details.account, details.amount, details.asset_code, details.asset_issuer, details.asset_type, details.authorize,
details.buying_asset_code, details.buying_asset_issuer, details.buying_asset_type, details.from, details.funder,
details.high_threshold, details.home_domain, details.inflation_dest, details.into, details.limit, details.low_threshold,
details.master_key_weight, details.med_threshold, details.name, details.offer_id, details.path,
details.price, details.price_r.d, details.price_r.n, details.selling_asset_code, details.selling_asset_issuer, details.selling_asset_type,
details.set_flags, details.set_flags_s, details.signer_key, details.signer_weight, details.source_amount, details.source_asset_code,
details.source_asset_issuer, details.source_asset_type, details.source_max, details.starting_balance, details.to, details.trustee,
details.trustor, details.value, details.clear_flags, details.clear_flags_s, details.destination_min, details.bump_to,
details.sponsor, details.sponsored_id, details.begin_sponsor,
-- operation fields
ho.application_order as op_application_order, ho.id as op_id, source_account as op_source_account,
source_account_muxed as op_source_account_muxed, transaction_id, type,
-- transaction fields
transaction_hash, ledger_sequence, ht.application_order as txn_application_order, ht.account as txn_account,
account_sequence, max_fee, ht.operation_count as txn_operation_count, ht.created_at as txn_created_at,
memo_type, memo, time_bounds, successful, fee_charged
-- ledger fields
ledger_hash, previous_ledger_hash, transaction_count, hl.operation_count as ledger_operation_count, closed_at,
hl.id as ledger_id, total_coins,
fee_pool, base_fee, base_reserve, max_tx_set_size, protocol_version, successful_transaction_count, failed_transaction_count,
ho.batch_id as batch_id, ho.batch_run_date as batch_run_date, current_timestamp() as batch_insert_ts
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