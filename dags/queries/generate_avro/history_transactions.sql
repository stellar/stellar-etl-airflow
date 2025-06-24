export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        id
        , transaction_hash
        , ledger_sequence
        , account
        , account_sequence
        , max_fee
        , operation_count
        , created_at
        , memo_type
        , memo
        , time_bounds
        , successful
        , fee_charged
        , inner_transaction_hash
        , fee_account
        , new_max_fee
        , account_muxed
        , fee_account_muxed
        , ledger_bounds
        , min_account_sequence
        , min_account_sequence_age
        , min_account_sequence_ledger_gap
        , tx_envelope
        , tx_result
        , tx_meta
        , tx_fee_meta
        , extra_signers
        , resource_fee
        , soroban_resources_instructions
        , soroban_resources_read_bytes
        , soroban_resources_write_bytes
        , closed_at
        , transaction_result_code
        , inclusion_fee_bid
        , inclusion_fee_charged
        , resource_fee_refund
        , non_refundable_resource_fee_charged
        , refundable_resource_fee_charged
        , rent_fee_charged
        , tx_signers
        , refundable_fee
    from {project_id}.{dataset_id}.history_transactions
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
