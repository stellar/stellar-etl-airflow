import datetime

from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_dbt_task import build_dbt_task

from airflow import DAG
from airflow.models.variable import Variable

init_sentry()

dag = DAG(
    'partnership_assets',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2022, 4, 1, 0, 0),
    description='This DAG runs dbt to create the partnership asset intermediate tables and aggregate tables.',
    schedule_interval='0 11 * * *', # Daily 10 AM UTC
    params={},
)

# tasks for partnership_assets__account_holders_activity_fact
int_partnership_assets__account_holders_activity = build_dbt_task(dag, 'int_partnership_assets__account_holders_activity')
partnership_assets__account_holders_activity_fact = build_dbt_task(dag, 'partnership_assets__account_holders_activity_fact')

# tasks for partnership_assets__asset_activity_fact
int_partnership_assets__payment_volume = build_dbt_task(dag, 'int_partnership_assets__payment_volume')
int_partnership_assets__trust_lines_latest = build_dbt_task(dag, 'int_partnership_assets__trust_lines_latest')
int_partnership_assets__market_cap = build_dbt_task(dag, 'int_partnership_assets__market_cap')
int_partnership_assets__active_asset_holders = build_dbt_task(dag, 'int_partnership_assets__active_asset_holders')
int_partnership_assets__history_operations_filtered_by_partnership_assets = build_dbt_task(dag, 'int_partnership_assets__history_operations_filtered_by_partnership_assets')
int_partnership_assets__active_payment_accounts = build_dbt_task(dag, 'int_partnership_assets__active_payment_accounts')
partnership_assets__asset_activity_fact = build_dbt_task(dag, 'partnership_assets__asset_activity_fact')

# DAG task graph
# graph for partnership_assets__account_holders_activity_fact
int_partnership_assets__account_holders_activity >> partnership_assets__account_holders_activity_fact

# graph for partnership_assets__asset_activity_fact
int_partnership_assets__payment_volume >> partnership_assets__asset_activity_fact
int_partnership_assets__trust_lines_latest >> int_partnership_assets__market_cap >> partnership_assets__asset_activity_fact
int_partnership_assets__trust_lines_latest >> int_partnership_assets__active_asset_holders >> partnership_assets__asset_activity_fact
int_partnership_assets__history_operations_filtered_by_partnership_assets >> int_partnership_assets__active_payment_accounts >> partnership_assets__asset_activity_fact
