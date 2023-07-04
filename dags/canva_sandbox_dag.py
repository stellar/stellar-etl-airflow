"""
Once per month, this DAG updates the Canvas sandbox dataset with transactions tables, state tables with history and views.
"""
import datetime
import json

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_bq_insert_job_sandbox_task import (
    build_bq_insert_job_sandbox,
    build_bq_update_table,
    build_check_table,
)
from stellar_etl_airflow.build_check_execution_date_task import path_to_execute
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "canvas_sandbox_dag",
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
dbt_tables = Variable.get("dbt_tables", deserialize_json=True)
project = Variable.get("bq_project")
dataset = Variable.get("bq_dataset")
dbt_dataset = Variable.get("dbt_mart_dataset")
sandbox_dataset = Variable.get("sandbox_dataset")
test_project_check = Variable.get("test_project_check")
end_task_name = "end_of_execution"


start_update_tables = path_to_execute(dag, "start_update_tables")
start_update_views = path_to_execute(dag, "start_update_views")
end_of_execution = path_to_execute(dag, end_task_name)

check_accounts = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["accounts"],
    f"update_{table_ids['accounts']}",
    f"create{table_ids['accounts']}",
)

check_signers = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["signers"],
    f"update_{table_ids['signers']}",
    f"create{table_ids['signers']}",
)

check_assets = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["assets"],
    f"update_{table_ids['assets']}",
    f"create{table_ids['assets']}",
)

check_claimable_balances = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["claimable_balances"],
    f"update_{table_ids['claimable_balances']}",
    f"create{table_ids['claimable_balances']}",
)

check_effects = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["effects"],
    f"update_{table_ids['effects']}",
    f"create{table_ids['effects']}",
)

check_ledgers = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["ledgers"],
    f"update_{table_ids['ledgers']}",
    f"create{table_ids['ledgers']}",
)

check_liquidity_pools = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["liquidity_pools"],
    f"update_{table_ids['liquidity_pools']}",
    f"create{table_ids['liquidity_pools']}",
)

check_offers = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["offers"],
    f"update_{table_ids['offers']}",
    f"create{table_ids['offers']}",
)

check_operations = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["operations"],
    f"update_{table_ids['operations']}",
    f"create{table_ids['operations']}",
)

check_trades = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["trades"],
    f"update_{table_ids['trades']}",
    f"create{table_ids['trades']}",
)

check_transactions = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["transactions"],
    f"update_{table_ids['transactions']}",
    f"create{table_ids['transactions']}",
)

check_trustlines = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["trustlines"],
    f"update_{table_ids['trustlines']}",
    f"create{table_ids['trustlines']}",
)

check_enriched_history_operations = build_check_table(
    dag,
    project,
    sandbox_dataset,
    table_ids["enriched_history_operations"],
    f"update_{table_ids['enriched_history_operations']}",
    f"create{table_ids['enriched_history_operations']}",
)

update_table_accounts = build_bq_update_table(
    dag, project, dataset, table_ids["accounts"], sandbox_dataset
)

update_table_signers = build_bq_update_table(
    dag, project, dataset, table_ids["signers"], sandbox_dataset
)

update_table_assets = build_bq_update_table(
    dag, project, dataset, table_ids["assets"], sandbox_dataset
)

update_table_claimable_balances = build_bq_update_table(
    dag, project, dataset, table_ids["claimable_balances"], sandbox_dataset
)

update_table_effects = build_bq_update_table(
    dag, project, dataset, table_ids["effects"], sandbox_dataset
)

update_table_ledgers = build_bq_update_table(
    dag, project, dataset, table_ids["ledgers"], sandbox_dataset
)

update_table_liquidity_pools = build_bq_update_table(
    dag, project, dataset, table_ids["liquidity_pools"], sandbox_dataset
)

update_table_offers = build_bq_update_table(
    dag, project, dataset, table_ids["offers"], sandbox_dataset
)

update_table_operations = build_bq_update_table(
    dag, project, dataset, table_ids["operations"], sandbox_dataset
)

update_table_trades = build_bq_update_table(
    dag, project, dataset, table_ids["trades"], sandbox_dataset
)

update_table_transactions = build_bq_update_table(
    dag, project, dataset, table_ids["transactions"], sandbox_dataset
)

update_table_trustlines = build_bq_update_table(
    dag, project, dataset, table_ids["trustlines"], sandbox_dataset
)

update_table_enriched_history_operations = build_bq_update_table(
    dag, project, dataset, table_ids["enriched_history_operations"], sandbox_dataset
)

create_accounts = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["accounts"], sandbox_dataset
)
create_assets = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["assets"], sandbox_dataset
)
create_claimable_balances = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["claimable_balances"], sandbox_dataset
)
create_effects = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["effects"], sandbox_dataset
)
create_ledgers = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["ledgers"], sandbox_dataset
)
create_liquidity_pools = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["liquidity_pools"], sandbox_dataset
)
create_offers = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["offers"], sandbox_dataset
)
create_operations = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["operations"], sandbox_dataset
)
create_signers = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["signers"], sandbox_dataset
)
create_trades = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["trades"], sandbox_dataset
)
create_transactions = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["transactions"], sandbox_dataset
)
create_trustlines = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["trustlines"], sandbox_dataset
)
create_enriched_history_operations = build_bq_insert_job_sandbox(
    dag, project, dataset, table_ids["enriched_history_operations"], sandbox_dataset
)

update_view_signers_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dbt_dataset,
    dbt_tables["signers_current"],
    sandbox_dataset,
)
update_view_accounts_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dbt_dataset,
    dbt_tables["accounts_current"],
    sandbox_dataset,
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
update_view_liquidity_pools_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dbt_dataset,
    dbt_tables["liquidity_pools_current"],
    sandbox_dataset,
)

update_view_liquidity_pool_value = path_to_execute(dag, "update_for_test")
check_view_liquidity_pool_value = path_to_execute(dag, "check_for_test")
if project != test_project_check:
    update_view_liquidity_pool_value = build_bq_insert_job_sandbox(
        dag,
        project,
        dataset,
        view_ids["v_liquidity_pool_value"],
        sandbox_dataset,
        view=True,
    )

    check_view_liquidity_pool_value = build_check_table(
        dag,
        project,
        sandbox_dataset,
        view_ids["v_liquidity_pool_value"],
        end_task_name,
        f"create_{view_ids['v_liquidity_pool_value']}",
    )
update_view_liquidity_providers = build_bq_insert_job_sandbox(
    dag, project, dataset, view_ids["v_liquidity_providers"], sandbox_dataset, view=True
)
update_view_offers_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dbt_dataset,
    dbt_tables["offers_current"],
    sandbox_dataset,
)
update_view_trustlines_current = build_bq_insert_job_sandbox(
    dag,
    project,
    dbt_dataset,
    dbt_tables["trustlines_current"],
    sandbox_dataset,
)

check_view_signers_current = build_check_table(
    dag,
    project,
    sandbox_dataset,
    dbt_tables["signers_current"],
    end_task_name,
    f"create_{dbt_tables['signers_current']}",
)
check_view_accounts_current = build_check_table(
    dag,
    project,
    sandbox_dataset,
    dbt_tables["accounts_current"],
    end_task_name,
    f"create_{dbt_tables['accounts_current']}",
)
check_view_claimable_balances_current = build_check_table(
    dag,
    project,
    sandbox_dataset,
    view_ids["v_claimable_balances_current"],
    end_task_name,
    f"create_{view_ids['v_claimable_balances_current']}",
)
check_view_liquidity_pool_trade = build_check_table(
    dag,
    project,
    sandbox_dataset,
    view_ids["v_liquidity_pool_trade"],
    end_task_name,
    f"create_{view_ids['v_liquidity_pool_trade']}",
)

check_view_liquidity_pools_current = build_check_table(
    dag,
    project,
    sandbox_dataset,
    dbt_tables["liquidity_pools_current"],
    end_task_name,
    f"create_{dbt_tables['liquidity_pools_current']}",
)
check_view_liquidity_providers = build_check_table(
    dag,
    project,
    sandbox_dataset,
    view_ids["v_liquidity_providers"],
    end_task_name,
    f"create_{view_ids['v_liquidity_providers']}",
)
check_view_offers_current = build_check_table(
    dag,
    project,
    sandbox_dataset,
    dbt_tables["offers_current"],
    end_task_name,
    f"create_{dbt_tables['offers_current']}",
)
check_view_trustlines_current = build_check_table(
    dag,
    project,
    sandbox_dataset,
    dbt_tables["trustlines_current"],
    end_task_name,
    f"create_{dbt_tables['trustlines_current']}",
)


start_update_tables >> [
    check_accounts,
    check_assets,
    check_claimable_balances,
    check_effects,
    check_ledgers,
    check_liquidity_pools,
    check_offers,
    check_operations,
    check_signers,
    check_trades,
    check_transactions,
    check_trustlines,
    check_enriched_history_operations,
]

check_accounts >> [create_accounts, update_table_accounts]

check_assets >> [create_assets, update_table_assets]

check_claimable_balances >> [create_claimable_balances, update_table_claimable_balances]

check_effects >> [create_effects, update_table_effects]

check_ledgers >> [create_ledgers, update_table_ledgers]

check_liquidity_pools >> [create_liquidity_pools, update_table_liquidity_pools]

check_offers >> [create_offers, update_table_offers]

check_operations >> [create_operations, update_table_operations]

check_signers >> [create_signers, update_table_signers]

check_trades >> [create_trades, update_table_trades]

check_transactions >> [create_transactions, update_table_transactions]

check_trustlines >> [create_trustlines, update_table_trustlines]

check_enriched_history_operations >> [
    create_enriched_history_operations,
    update_table_enriched_history_operations,
]

start_update_views >> [
    check_view_signers_current,
    check_view_signers_current,
    check_view_accounts_current,
    check_view_claimable_balances_current,
    check_view_liquidity_pool_trade,
    check_view_liquidity_pool_value,
    check_view_liquidity_pools_current,
    check_view_liquidity_providers,
    check_view_offers_current,
    check_view_trustlines_current,
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

check_view_trustlines_current >> [update_view_trustlines_current, end_of_execution]
