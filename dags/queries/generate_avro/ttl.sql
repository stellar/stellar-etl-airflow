export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        key_hash
        , live_until_ledger_seq
        , last_modified_ledger
        , ledger_entry_change
        , deleted
        , batch_id
        , batch_run_date
        , batch_insert_ts
        , closed_at
        , ledger_sequence
    from {project_id}.{dataset_id}.ttl
    where
        true
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
