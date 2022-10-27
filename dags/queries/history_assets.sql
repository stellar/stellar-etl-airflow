/* This query prepares the assets of each load for deduplication, in order to guarantee a new asset won't be loaded twice */
WITH new_load as (
  select
    *
    , row_number() OVER(PARTITION BY asset_type, asset_issuer, asset_code ORDER BY batch_run_date ASC) as dedup_oldest_asset
  from `{project_id}.{dataset_id}.history_assets_staging`
)

/* Deduplicates the new batch assets, guaranteeing they are unique within the batch */
, new_load_dedup as (
  select *
  from new_load
  where dedup_oldest_asset = 1
)

/* selects the deduplicated table for comparision */
, deduplicated_table as (
 SELECT *
 FROM `{project_id}.{dataset_id}.history_assets`
)

/* excludes any assets from the load that already exist in the deduplicated table */
, exclude_duplicates as (
  select
    new_load_dedup.asset_code
    , new_load_dedup.asset_issuer
    , new_load_dedup.asset_type
  from new_load_dedup
  left join deduplicated_table on
    new_load_dedup.asset_code = deduplicated_table.asset_code
    and new_load_dedup.asset_issuer = deduplicated_table.asset_issuer
    and new_load_dedup.asset_type = deduplicated_table.asset_type
  where deduplicated_table.asset_code is null
    and deduplicated_table.asset_issuer is null
    and deduplicated_table.asset_type is null
)

/* Adds only the new assets from the load to the assets table */
SELECT
 new_load_dedup.id
 , exclude_duplicates.asset_type
 , exclude_duplicates.asset_code
 , exclude_duplicates.asset_issuer
 , new_load_dedup.batch_id
 , new_load_dedup.batch_run_date
 , new_load_dedup.batch_insert_ts
FROM exclude_duplicates
left join new_load_dedup on
  exclude_duplicates.asset_type = new_load_dedup.asset_type
  and exclude_duplicates.asset_code = new_load_dedup.asset_code
  and exclude_duplicates.asset_issuer = new_load_dedup.asset_issuer
select *
from deduplicated_table