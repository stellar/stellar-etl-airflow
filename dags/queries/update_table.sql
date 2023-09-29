insert into {target_project_id}.{target_dataset}.{table_id}
select *
from {project_id}.{dataset_id}.{table_id}
where date_trunc(batch_run_date, day) = date_trunc(current_date() - interval 1 day, day)
