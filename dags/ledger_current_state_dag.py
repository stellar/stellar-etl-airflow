import datetime

from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_dbt_task import build_dbt_task

from airflow import DAG
from airflow.models.variable import Variable

#init_sentry()

dag = DAG(
    'ledger_current_state',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 4, 0, 0),
    description='This DAG runs dbt to create the tables for the models in marts/ledger_current_state/',
    schedule_interval='0 */1 * * *', # Runs hourly
    params={},
    catchup=False,
)

# tasks for ledger_current_state tables
account_signers_current = build_dbt_task(dag, 'account_signers_current')
accounts_current = build_dbt_task(dag, 'accounts_current')
liquidity_pools_current = build_dbt_task(dag, 'liquidity_pools_current')
offers_current = build_dbt_task(dag, 'offers_current')
trust_lines_current = build_dbt_task(dag, 'trust_lines_current')

# DAG task graph
# graph for ledger_current_state tables
account_signers_current
accounts_current
liquidity_pools_current
offers_current
trust_lines_current
