create or replace view `{target_project}.{target_dataset}.{table_id}` as (
    select * from `{project_id}.{dataset_id}.{table_id}`
)
