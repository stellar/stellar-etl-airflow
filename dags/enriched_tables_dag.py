import datetime

from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_dbt_task import build_dbt_task

from airflow import DAG
from airflow.models.variable import Variable

#init_sentry()

dag = DAG(
    'enriched_tables',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 4, 0, 0),
    description='This DAG runs dbt to create the tables for the models in marts/enriched/.',
    schedule_interval='0 */1 * * *', # Runs hourly
    params={},
    catchup=False,
)

# tasks for enriched tables
enriched_history_operations = build_dbt_task(dag, 'enriched_history_operations')
enriched_history_operations_meaningful = build_dbt_task(dag, 'enriched_history_operations_meaningful')

# DAG task graph
# graph for enriched tables
enriched_history_operations >> enriched_history_operations_meaningful
