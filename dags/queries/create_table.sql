create or replace table `{project_id}.{target_dataset}.{table_id}`
partition by date_trunc(batch_run_date, day)
options (partition_expiration_days = 180) as (
    select *
    from `{project_id}.{dataset_id}.{table_id}`
    where batch_run_date >= date_sub(current_date(), interval 6 month)
)
