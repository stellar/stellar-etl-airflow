import datetime

from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_dbt_task import build_dbt_task

from airflow import DAG
from airflow.models.variable import Variable

init_sentry()

dag = DAG(
    'marts_tables',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 4, 0, 0),
    description='This DAG runs dbt to create the tables for the models in marts/ but not any marts subdirectories.',
    schedule_interval='0 11 * * *', # Daily 11 AM UTC
    params={},
    catchup=False,
)

# tasks for marts tables
agg_network_stats = build_dbt_task(dag, 'agg_network_stats')
fee_stats_agg = build_dbt_task(dag, 'fee_stats_agg')
history_assets = build_dbt_task(dag, 'history_assets')
trade_agg = build_dbt_task(dag, 'trade_agg')

# DAG task graph
# graph for marts tables
agg_network_stats
fee_stats_agg
history_assets
trade_agg
