export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          transaction_hash
        , transaction_id
        , successful
        , in_successful_contract_call
        , contract_id
        , type
        , type_string
        , topics
        , topics_decoded
        , data
        , data_decoded
        , contract_event_xdr
        , closed_at
        , ledger_sequence
    from {project_id}.{dataset_id}.history_contract_events
    where
        true
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
