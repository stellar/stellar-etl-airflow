export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
          contract_id
        , contract_key_type
        , contract_durability
        , asset_issuer
        , asset_type
        , balance_holder
        , balance
        , last_modified_ledger
        , ledger_entry_change
        , deleted
        , closed_at
        , ledger_sequence
        , ledger_key_hash
        , key
        , key_decoded
        , val
        , val_decoded
        , contract_data_xdr
        , ledger_key_hash_base_64
        , replace(asset_code, '\u0000', '') as asset_code
    from {project_id}.{dataset_id}.contract_data
    where
        true
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
