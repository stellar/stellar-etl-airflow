export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        id
        , source_account
        , source_account_muxed
        , transaction_id
        , type
        , type_string
        , closed_at
        , operation_result_code
        , operation_trace_code
        , ledger_sequence
        , details.account
        , details.account_muxed
        , details.account_muxed_id
        , details.account_id
        , details.amount
        , details.asset
        , details.asset_code
        , details.asset_issuer
        , details.asset_id
        , details.asset_type
        , details.authorize
        , details.balance_id
        , details.balance_id_strkey
        , details.buying_asset_code
        , details.buying_asset_issuer
        , details.buying_asset_id
        , details.buying_asset_type
        , details.claimable_balance_id
        , details.claimable_balance_id_strkey
        , details.claimant
        , details.claimant_muxed
        , details.claimant_muxed_id
        , details.data_account_id
        , details.data_name
        , details.from
        , details.from_muxed
        , details.from_muxed_id
        , details.funder
        , details.funder_muxed
        , details.funder_muxed_id
        , details.high_threshold
        , details.home_domain
        , details.inflation_dest
        , details.into
        , details.into_muxed
        , details.into_muxed_id
        , details.limit
        , details.low_threshold
        , details.master_key_weight
        , details.med_threshold
        , details.name
        , details.offer_id
        , details.path
        , details.price
        , details.price_r
        , details.selling_asset_code
        , details.selling_asset_issuer
        , details.selling_asset_id
        , details.selling_asset_type
        , details.set_flags
        , details.set_flags_s
        , details.signer_account_id
        , details.signer_key
        , details.signer_weight
        , details.source_amount
        , details.source_asset_code
        , details.source_asset_issuer
        , details.source_asset_id
        , details.source_asset_type
        , details.source_max
        , details.starting_balance
        , details.to
        , details.to_muxed
        , details.to_muxed_id
        , details.trustee
        , details.trustee_muxed
        , details.trustee_muxed_id
        , details.trustline_account_id
        , details.trustline_asset
        , details.trustor
        , details.trustor_muxed
        , details.trustor_muxed_id
        , details.value
        , details.clear_flags
        , details.clear_flags_s
        , details.destination_min
        , details.bump_to
        , details.authorize_to_maintain_liabilities
        , details.clawback_enabled
        , details.sponsor
        , details.sponsored_id
        , details.begin_sponsor
        , details.begin_sponsor_muxed
        , details.begin_sponsor_muxed_id
        , details.liquidity_pool_id
        , details.liquidity_pool_id_strkey
        , details.reserve_a_asset_type
        , details.reserve_a_asset_code
        , details.reserve_a_asset_issuer
        , details.reserve_a_asset_id
        , details.reserve_a_max_amount
        , details.reserve_a_deposit_amount
        , details.reserve_b_asset_type
        , details.reserve_b_asset_code
        , details.reserve_b_asset_issuer
        , details.reserve_b_asset_id
        , details.reserve_b_max_amount
        , details.reserve_b_deposit_amount
        , details.min_price
        , details.min_price_r
        , details.max_price
        , details.max_price_r
        , details.shares_received
        , details.reserve_a_min_amount
        , details.reserve_a_withdraw_amount
        , details.reserve_b_min_amount
        , details.reserve_b_withdraw_amount
        , details.shares
        , details.asset_balance_changes
        , details.parameters
        , details.parameters_decoded
        , details.function
        , details.address
        , details.type as soroban_operation_type
        , details.extend_to
        , details.contract_id
        , details.contract_code_hash
        , details.ledger_key_hash
        , details.parameters_json
        , details.parameters_json_decoded
        , details.ledgers_to_expire
    from {project_id}.{dataset_id}.history_operations
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
