import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_copy_table_task import build_copy_table
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "ledger_current_state",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 4, 0, 0),
    description="This DAG runs dbt to create the tables for the models in marts/ledger_current_state/",
    schedule_interval="0 */1 * * *",  # Runs hourly; NOTE: This can be changed to daily if execution time is too slow
    params={},
    catchup=False,
)

internal_project = Variable.get("bq_project")
internal_dataset = Variable.get("dbt_mart_dataset")
public_project = Variable.get("public_project")
public_dataset = Variable.get("public_dataset_new")

# tasks for staging tables for ledger_current_state tables
stg_account_signers = build_dbt_task(dag, "stg_account_signers")
stg_history_ledgers = build_dbt_task(dag, "stg_history_ledgers")
stg_accounts = build_dbt_task(dag, "stg_accounts")
stg_liquidity_pools = build_dbt_task(dag, "stg_liquidity_pools")
stg_offers = build_dbt_task(dag, "stg_offers")
stg_trust_lines = build_dbt_task(dag, "stg_trust_lines")

# tasks for ledger_current_state tables
account_signers = "account_signers_current"
account_signers_current = build_dbt_task(dag, account_signers)
accounts = "accounts_current"
accounts_current = build_dbt_task(dag, accounts)
liquidity_pools = "liquidity_pools_current"
liquidity_pools_current = build_dbt_task(dag, liquidity_pools)
offers = "offers_current"
offers_current = build_dbt_task(dag, offers)
trust_lines = "trust_lines_current"
trust_lines_current = build_dbt_task(dag, trust_lines)

# copy the tables over to public dataset
account_signers_public = build_copy_table(
    dag,
    internal_project,
    internal_dataset,
    account_signers,
    public_project,
    public_dataset,
    account_signers,
)
accounts_public = build_copy_table(
    dag,
    internal_project,
    internal_dataset,
    accounts,
    public_project,
    public_dataset,
    accounts,
)
liquidity_pools_public = build_copy_table(
    dag,
    internal_project,
    internal_dataset,
    liquidity_pools,
    public_project,
    public_dataset,
    liquidity_pools,
)
offers_public = build_copy_table(
    dag,
    internal_project,
    internal_dataset,
    offers,
    public_project,
    public_dataset,
    offers,
)
trust_lines_public = build_copy_table(
    dag,
    internal_project,
    internal_dataset,
    trust_lines,
    public_project,
    public_dataset,
    trust_lines,
)

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

account_signers_current >> account_signers_public
accounts_current >> accounts_public
liquidity_pools_current >> liquidity_pools_public
offers_current >> offers_public
trust_lines_current >> trust_lines_public
