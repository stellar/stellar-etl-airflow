export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        *
        except (batch_id, batch_insert_ts, batch_run_date, asset_code)
        , replace(asset_code, '\u0000', '') as asset_code
    from {project_id}.{dataset_id}.contract_data
    where
        true
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
