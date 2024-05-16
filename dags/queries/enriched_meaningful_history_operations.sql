/*
Query filters the enriched_history_operations table down to meaningful/relevant operations
only. If any asset that is a part of the operation is on the current meaningful asset list,
the operation is included in the table.

Table is heavily used for Partner Metabase Dashboards.

NOTE: relevant assets are proprietary internal data and should not be shared externally.
*/
select
    eho.*
    , ma.code
    , ma.issuer
from {project_id}.{dataset_id}.enriched_history_operations as eho
inner join
    {project_id}.{dataset_id}.meaningful_assets as ma
    on
    eho.asset_id = farm_fingerprint(concat(ma.code, ma.issuer, ma.type))
    or eho.source_asset_id = farm_fingerprint(concat(ma.code, ma.issuer, ma.type))
    or eho.selling_asset_id = farm_fingerprint(concat(ma.code, ma.issuer, ma.type))
    or eho.buying_asset_id = farm_fingerprint(concat(ma.code, ma.issuer, ma.type))
where
    eho.batch_id = '{batch_id}'
    and eho.batch_run_date = '{batch_run_date}'
