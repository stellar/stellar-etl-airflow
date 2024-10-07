export data
  options (
    uri = '{uri}',
    format = 'avro',
    overwrite=true
    )
as (
  select
    *
    except(sequence_ledger, batch_id, batch_insert_ts, batch_run_date),
    sequence_ledger as account_sequence_last_modified_ledger
  from {project_id}.{dataset_id}.accounts
  where true
    and closed_at >= '{batch_run_date}'
    and closed_at < '{next_batch_run_date}'
  order by closed_at asc
)
