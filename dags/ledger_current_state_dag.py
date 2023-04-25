import datetime

from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_dbt_task import build_dbt_task

from airflow import DAG
from airflow.models.variable import Variable

init_sentry()

dag = DAG(
    'ledger_current_state',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 4, 0, 0),
    description='This DAG runs dbt to create the tables for the models in marts/ledger_current_state/',
    schedule_interval='0 */1 * * *', # Runs hourly; NOTE: This can be changed to daily if execution time is too slow
    params={},
    catchup=False,
)

# tasks for staging tables for ledger_current_state tables
stg_account_signers = build_dbt_task(dag, 'stg_account_signers')
stg_history_ledgers = build_dbt_task(dag, 'stg_history_ledgers')
stg_accounts = build_dbt_task(dag, 'stg_accounts')
stg_liquidity_pools = build_dbt_task(dag, 'stg_liquidity_pools')
stg_offers = build_dbt_task(dag, 'stg_offers')
stg_trust_lines = build_dbt_task(dag, 'stg_trust_lines')

# tasks for ledger_current_state tables
account_signers_current = build_dbt_task(dag, 'account_signers_current')
accounts_current = build_dbt_task(dag, 'accounts_current')
liquidity_pools_current = build_dbt_task(dag, 'liquidity_pools_current')
offers_current = build_dbt_task(dag, 'offers_current')
trust_lines_current = build_dbt_task(dag, 'trust_lines_current')

# DAG task graph
# graph for ledger_current_state tables
stg_account_signers >> account_signers_current
stg_history_ledgers >> account_signers_current

stg_accounts >> accounts_current
stg_history_ledgers >> accounts_current

stg_liquidity_pools >> liquidity_pools_current
stg_history_ledgers >> liquidity_pools_current

stg_offers >> offers_current
stg_history_ledgers >> offers_current

stg_trust_lines >> trust_lines_current
stg_history_ledgers >> trust_lines_current
