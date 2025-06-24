export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          account_id
        , balance
        , buying_liabilities
        , selling_liabilities
        , sequence_number
        , num_subentries
        , inflation_destination
        , flags
        , home_domain
        , master_weight
        , threshold_low
        , threshold_medium
        , threshold_high
        , last_modified_ledger
        , ledger_entry_change
        , deleted
        , sponsor
        , num_sponsored
        , num_sponsoring
        , sequence_time
        , closed_at
        , ledger_sequence
        , sequence_ledger as account_sequence_last_modified_ledger
    from {project_id}.{dataset_id}.accounts
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
