insert into `{target_project}.{target_dataset}.{table_id}`
select *
from `{project_id}.{dataset_id}.{table_id}`
where date_trunc(batch_run_date, day) = date_trunc(cast('{batch_run_date}' as datetime), day)
