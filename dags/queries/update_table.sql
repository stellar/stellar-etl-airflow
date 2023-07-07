insert into {project_id}.{target_dataset}.{table_id}
select *
from {project_id}.{dataset_id}.{table_id}
where date_trunc(batch_run_date, month) = date_trunc(current_date() - interval 1 month, month)
