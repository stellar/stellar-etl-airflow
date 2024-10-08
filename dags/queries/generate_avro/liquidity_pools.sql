export data
options (
    uri = '{uri}'
    , format = 'avro'
    , overwrite = true
)
as (
    select
        *
        except (batch_id, batch_insert_ts, batch_run_date)
    from {project_id}.{dataset_id}.liquidity_pools
    where
        true
        and batch_run_date >= '{batch_run_date}'
        and batch_run_date < '{next_batch_run_date}'
        and closed_at >= '{batch_run_date}'
        and closed_at < '{next_batch_run_date}'
    order by closed_at asc
)
