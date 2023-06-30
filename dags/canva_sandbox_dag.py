"""
Once per month, this DAG updates the Canvas sandbox dataset with transactions tables, state tables with history and views.
"""
import datetime
import json

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_bq_insert_job_sandbox_task import (
    build_bq_insert_job_sandbox,
    build_check_view,
)
from stellar_etl_airflow.build_check_execution_date_task import path_to_execute
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "canvas_sandbox",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 1, 1),
    description="This DAG update .",
    schedule_interval="0 0 1 * *",
    params={
        "alias": "canvas-sandbox",
    },
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    catchup=False,
)

table_ids = Variable.get("table_ids", deserialize_json=True)
view_ids = Variable.get("view_ids", deserialize_json=True)
project = Variable.get("bq_project")
dataset = Variable.get("bq_dataset")
sandbox_dataset = Variable.get("sandbox_dataset")
sandbox_dataset = "canva_sandbox"

start_update_tables = path_to_execute(dag, "start_update_tables")
start_update_views = path_to_execute(dag, "start_update_views")
end_of_execution = path_to_execute(dag, "end_of_execution")

update_accounts = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["accounts"], sandbox_dataset
)
update_assets = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["assets"], sandbox_dataset
)
update_claimable_balances = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["claimable_balances"], sandbox_dataset
)
update_effects = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["effects"], sandbox_dataset
)
update_ledgers = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["ledgers"], sandbox_dataset
)
update_liquidity_pools = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["liquidity_pools"], sandbox_dataset
)
update_offers = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["offers"], sandbox_dataset
)
update_operations = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["operations"], sandbox_dataset
)
update_signers = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["signers"], sandbox_dataset
)
update_trades = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["trades"], sandbox_dataset
)
update_transactions = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["transactions"], sandbox_dataset
)
update_trustlines = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["trustlines"], sandbox_dataset
)
update_enriched_history_operations = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["enriched_history_operations"], sandbox_dataset
)

update_view_signers_current = build_bq_insert_job_sandbox(
    dag, project, dataset, view_ids["v_signers_current"], sandbox_dataset, view=True
)
update_view_accounts_current = build_bq_insert_job_sandbox(
    dag, project, dataset, view_ids["v_accounts_current"], sandbox_dataset, view=True
)
update_view_claimable_balances_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dataset,
    view_ids["v_claimable_balances_current"],
    sandbox_dataset,
    view=True,
)
update_view_liquidity_pool_trade = build_bq_insert_job_sandbox(
    dag,
    project,
    dataset,
    view_ids["v_liquidity_pool_trade"],
    sandbox_dataset,
    view=True,
)
update_view_liquidity_pool_value = build_bq_insert_job_sandbox(
    dag,
    project,
    dataset,
    view_ids["v_liquidity_pool_value"],
    sandbox_dataset,
    view=True,
)
update_view_liquidity_pools_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dataset,
    view_ids["v_liquidity_pools_current"],
    sandbox_dataset,
    view=True,
)
update_view_liquidity_providers = build_bq_insert_job_sandbox(
    dag, project, dataset, view_ids["v_liquidity_providers"], sandbox_dataset, view=True
)
update_view_offers_current = build_bq_insert_job_sandbox(
    dag, project, dataset, view_ids["v_offers_current"], sandbox_dataset, view=True
)
update_view_trust_lines_current = build_bq_insert_job_sandbox(
    dag, project, dataset, view_ids["v_trust_lines_current"], sandbox_dataset, view=True
)

check_view_signers_current = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_signers_current"]
)
check_view_accounts_current = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_accounts_current"]
)
check_view_claimable_balances_current = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_claimable_balances_current"]
)
check_view_liquidity_pool_trade = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_liquidity_pool_trade"]
)
check_view_liquidity_pool_value = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_liquidity_pool_value"]
)
check_view_liquidity_pools_current = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_liquidity_pools_current"]
)
check_view_liquidity_providers = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_liquidity_providers"]
)
check_view_offers_current = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_offers_current"]
)
check_view_trust_lines_current = build_check_view(
    dag, project, sandbox_dataset, view_ids["v_trust_lines_current"]
)


start_update_tables >> [
    update_accounts,
    update_assets,
    update_claimable_balances,
    update_effects,
    update_ledgers,
    update_liquidity_pools,
    update_offers,
    update_operations,
    update_signers,
    update_trades,
    update_transactions,
    update_trustlines,
    update_enriched_history_operations,
]

start_update_views >> [
    check_view_signers_current,
    check_view_signers_current,
    check_view_accounts_current,
    check_view_claimable_balances_current,
    check_view_liquidity_pool_trade,
    #    check_view_liquidity_pool_value,
    check_view_liquidity_pools_current,
    check_view_liquidity_providers,
    check_view_offers_current,
    check_view_trust_lines_current,
]

check_view_signers_current >> [update_view_signers_current, end_of_execution]

check_view_accounts_current >> [update_view_accounts_current, end_of_execution]

check_view_claimable_balances_current >> [
    update_view_claimable_balances_current,
    end_of_execution,
]

check_view_liquidity_pool_trade >> [update_view_liquidity_pool_trade, end_of_execution]

check_view_liquidity_pool_value >> [update_view_liquidity_pool_value, end_of_execution]

check_view_liquidity_pools_current >> [
    update_view_liquidity_pools_current,
    end_of_execution,
]

check_view_liquidity_providers >> [update_view_liquidity_providers, end_of_execution]

check_view_offers_current >> [update_view_offers_current, end_of_execution]

check_view_trust_lines_current >> [update_view_trust_lines_current, end_of_execution]
