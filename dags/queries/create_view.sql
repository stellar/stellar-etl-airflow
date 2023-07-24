create or replace view `{project_id}.{target_dataset}.{table_id}` as (
    select * from `{project_id}.{dataset_id}.{table_id}`
)
