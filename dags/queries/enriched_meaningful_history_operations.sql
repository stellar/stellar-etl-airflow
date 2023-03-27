/*
Query filters the enriched_history_operations table down to meaningful/relevant operations
only. If any asset that is a part of the operation is on the current meaningful asset list,
the operation is included in the table.

Table is heavily used for Partner Metabase Dashboards.

NOTE: relevant assets are proprietary internal data and should not be shared externally.
*/
SELECT
  eho.*
  , ma.code
  , ma.issuer
FROM `{project_id}.{dataset_id}.enriched_history_operations` eho
INNER JOIN `{project_id}.{dataset_id}.meaningful_assets` ma ON
  eho.asset_id = FARM_FINGERPRINT(CONCAT(ma.code, ma.issuer, ma.`type`)) OR
  eho.source_asset_id = FARM_FINGERPRINT(CONCAT(ma.code, ma.issuer, ma.`type`)) OR
  eho.selling_asset_id = FARM_FINGERPRINT(CONCAT(ma.code, ma.issuer, ma.`type`)) OR
  eho.buying_asset_id = FARM_FINGERPRINT(CONCAT(ma.code, ma.issuer, ma.`type`))
WHERE eho.batch_id = '{batch_id}'
    AND eho.batch_run_date = '{batch_run_date}'
