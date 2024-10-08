export data
  options (
    uri = '{uri}',
    format = 'avro',
    overwrite=true
    )
as (
  select
    *
    except(ledger_closed_at, batch_id, batch_insert_ts, batch_run_date),
    ledger_closed_at as closed_at
  from {project_id}.{dataset_id}.history_trades
  where true
    and ledger_closed_at >= '{batch_run_date}'
    and ledger_closed_at < '{next_batch_run_date}'
  order by closed_at asc
)
