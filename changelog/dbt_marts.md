# Changes in DBT marts schema

| Date       |       Table Name                | Operation     | Columns                  |
|------------|---------------------------------|---------------|--------------------------|
| 2024-10-29 | ENRICHED_HISTORY_OPERATIONS_MEANINGFUL | type_changed | min_price_r, claimants, path, asset_balance_changes, parameters_decoded, parameters, max_price_r |
| 2024-10-29 | ENRICHED_HISTORY_OPERATIONS_MEANINGFUL | column_added | airflow_start_ts, details_json |
| 2024-10-29 | ENRICHED_HISTORY_OPERATIONS_MGI | type_changed | claimants, asset_balance_changes, parameters_decoded, max_price_r, min_price_r, parameters, path |
| 2024-10-29 | ENRICHED_HISTORY_OPERATIONS_MGI | column_added | details_json, airflow_start_ts |
| 2024-10-29 | ENRICHED_HISTORY_OPERATIONS_XLM | column_added | details_json, airflow_start_ts |
| 2024-10-29 | ENRICHED_HISTORY_OPERATIONS_XLM | type_changed | parameters, max_price_r, min_price_r, path, asset_balance_changes, parameters_decoded, claimants |
| 2024-09-12 | ASSET_STATS_AGG | column_added | airflow_start_ts |
| 2024-09-12 | DIM_DATES | column_added | airflow_start_ts |
| 2024-09-12 | DIM_MGI_WALLETS | column_added | airflow_start_ts |
| 2024-09-12 | FCT_MGI_CASHFLOW | column_added | airflow_start_ts |
| 2024-09-12 | LIQUIDITY_POOLS_VALUE | column_added | airflow_start_ts |
| 2024-09-12 | LIQUIDITY_POOLS_VALUE_HISTORY | column_added | airflow_start_ts |
| 2024-09-12 | LIQUIDITY_POOL_TRADE_VOLUME | column_added | airflow_start_ts |
| 2024-09-12 | LIQUIDITY_PROVIDERS | column_added | airflow_start_ts |
| 2024-09-12 | MGI_MONTHLY_USD_BALANCE | column_added | airflow_start_ts |
| 2024-09-12 | MGI_NETWORK_STATS_AGG | column_added | airflow_start_ts |
| 2024-09-12 | NETWORK_STATS_AGG | column_added | airflow_start_ts |
| 2024-09-12 | OHLC_EXCHANGE_FACT | column_added | airflow_start_ts |
| 2024-09-12 | PARTNERSHIP_ASSETS__ACCOUNT_HOLDERS_ACTIVITY_FACT | column_added | airflow_start_ts |
| 2024-09-12 | PARTNERSHIP_ASSETS__ASSET_ACTIVITY_FACT | column_added | airflow_start_ts |
| 2024-09-12 | PARTNERSHIP_ASSETS__MOST_ACTIVE_FACT | column_added | airflow_start_ts |
