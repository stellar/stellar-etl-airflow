create or replace table `{target_project}.{target_dataset}.{table_id}`
partition by date_trunc(batch_run_date, day)
options (partition_expiration_days = 450) as (
    select *
    from `{project_id}.{dataset_id}.{table_id}`
    where
        batch_run_date >= date_trunc(date_sub('{batch_run_date}', interval 15 month), day)
        and batch_run_date < date_trunc('{batch_run_date}', day)
)
