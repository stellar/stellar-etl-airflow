import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "partnership_assets",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2022, 4, 1, 0, 0),
    description="This DAG runs dbt to create the partnership asset intermediate tables and aggregate tables.",
    schedule_interval="0 10 * * *",  # Daily 10 AM UTC
    params={},
    max_active_runs=1,
)

# tasks for staging tables for partnership assets
stg_partnership_assets = build_dbt_task(dag, "stg_partnership_assets")
stg_partnership_asset_prices = build_dbt_task(dag, "stg_partnership_asset_prices")
stg_stellar_expert_account_tagging_information_raw = build_dbt_task(
    dag, "stg_stellar_expert_account_tagging_information_raw"
)
stg_trust_lines = build_dbt_task(dag, "stg_trust_lines")
stg_history_ledgers = build_dbt_task(dag, "stg_history_ledgers")
stg_history_operations = build_dbt_task(dag, "stg_history_operations")

# tasks for partnership_assets__account_holders_activity_fact
int_partnership_assets__account_holders_activity = build_dbt_task(
    dag, "int_partnership_assets__account_holders_activity"
)
partnership_assets__account_holders_activity_fact = build_dbt_task(
    dag, "partnership_assets__account_holders_activity_fact"
)

# tasks for partnership_assets__asset_activity_fact
int_partnership_assets__payment_volume = build_dbt_task(
    dag, "int_partnership_assets__payment_volume"
)
int_partnership_assets__trust_lines_latest = build_dbt_task(
    dag, "int_partnership_assets__trust_lines_latest"
)
int_partnership_assets__market_cap = build_dbt_task(
    dag, "int_partnership_assets__market_cap"
)
int_partnership_assets__active_asset_holders = build_dbt_task(
    dag, "int_partnership_assets__active_asset_holders"
)
int_partnership_assets__history_operations_filtered_by_partnership_assets = (
    build_dbt_task(
        dag, "int_partnership_assets__history_operations_filtered_by_partnership_assets"
    )
)
int_partnership_assets__active_payment_accounts = build_dbt_task(
    dag, "int_partnership_assets__active_payment_accounts"
)
partnership_assets__asset_activity_fact = build_dbt_task(
    dag, "partnership_assets__asset_activity_fact"
)

# DAG task graph
# graph for partnership_assets__account_holders_activity_fact
stg_partnership_assets >> int_partnership_assets__account_holders_activity
stg_partnership_asset_prices >> int_partnership_assets__account_holders_activity
(
    stg_stellar_expert_account_tagging_information_raw
    >> int_partnership_assets__account_holders_activity
)

(
    int_partnership_assets__account_holders_activity
    >> partnership_assets__account_holders_activity_fact
)

# graph for partnership_assets__asset_activity_fact
stg_partnership_assets >> int_partnership_assets__payment_volume
stg_partnership_asset_prices >> int_partnership_assets__payment_volume

stg_trust_lines >> int_partnership_assets__trust_lines_latest
stg_history_ledgers >> int_partnership_assets__trust_lines_latest
stg_partnership_assets >> int_partnership_assets__trust_lines_latest

stg_partnership_asset_prices >> int_partnership_assets__market_cap
int_partnership_assets__trust_lines_latest >> int_partnership_assets__market_cap

stg_partnership_asset_prices >> int_partnership_assets__active_asset_holders
(
    int_partnership_assets__trust_lines_latest
    >> int_partnership_assets__active_asset_holders
)

(
    stg_history_operations
    >> int_partnership_assets__history_operations_filtered_by_partnership_assets
)
(
    stg_partnership_assets
    >> int_partnership_assets__history_operations_filtered_by_partnership_assets
)
stg_partnership_assets >> int_partnership_assets__active_payment_accounts
(
    int_partnership_assets__history_operations_filtered_by_partnership_assets
    >> int_partnership_assets__active_payment_accounts
)

stg_partnership_assets >> partnership_assets__asset_activity_fact
int_partnership_assets__payment_volume >> partnership_assets__asset_activity_fact
int_partnership_assets__market_cap >> partnership_assets__asset_activity_fact
int_partnership_assets__active_asset_holders >> partnership_assets__asset_activity_fact
(
    int_partnership_assets__active_payment_accounts
    >> partnership_assets__asset_activity_fact
)
