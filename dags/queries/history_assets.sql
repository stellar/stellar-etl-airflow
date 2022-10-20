/*
This query deduplicates the table history_assets.
*/
WITH raw as (
  SELECT
    *
    , row_number() OVER(PARTITION BY asset_type, asset_issuer, asset_code ORDER BY batch_run_date ASC) as dedup_oldest_asset
  FROM `{project_id}.{dataset_id}.history_assets_staging`
)

SELECT
  id
  , asset_type
  , asset_code
  , asset_issuer
  , batch_run_date
  , batch_id
  , batch_insert_ts
FROM raw
WHERE dedup_oldest_asset = 1