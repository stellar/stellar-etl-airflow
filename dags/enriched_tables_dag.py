import datetime

from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps

from airflow import DAG
from airflow.models.variable import Variable

init_sentry()

dag = DAG(
    'enriched_tables',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 12, 0, 0),
    description='This DAG runs dbt to create the tables for the models in marts/enriched/.',
    schedule_interval='*/30 * * * *', # Runs every 30 mins
    params={},
    max_active_runs=1,
    catchup=False
)


# Batch loading of derived table, `enriched_history_operations` which denormalizes ledgers, transactions and operations data.
# Must wait on history_archive_without_captive_core_dag to finish before beginning the job.
wait_on_dag = build_cross_deps(
    dag, "wait_on_ledgers_txs", "history_archive_without_captive_core"
)

# tasks for staging tables
stg_history_ledgers = build_dbt_task(dag, 'stg_history_ledgers')
stg_history_transactions = build_dbt_task(dag, 'stg_history_transactions')
stg_history_operations = build_dbt_task(dag, 'stg_history_operations')
stg_meaningful_assets = build_dbt_task(dag, 'stg_meaningful_assets')

# tasks for enriched tables
enriched_history_operations = build_dbt_task(dag, 'enriched_history_operations')
enriched_history_operations_meaningful = build_dbt_task(dag, 'enriched_history_operations_meaningful')

# DAG task graph
# graph for enriched tables
wait_on_dag >> stg_history_ledgers >> enriched_history_operations
wait_on_dag >> stg_history_transactions >> enriched_history_operations
wait_on_dag >> stg_history_operations >> enriched_history_operations

enriched_history_operations >> enriched_history_operations_meaningful
stg_meaningful_assets >> enriched_history_operations_meaningful
