"""
When the Test net server is reset, the dataset reset DAG deletes all the datasets in the test Hubble.
"""

from ast import literal_eval
from datetime import datetime
from json import loads

from airflow import DAG
from airflow.models import Variable
from stellar_etl_airflow.build_check_execution_date_task import (
    build_check_execution_date,
    path_to_execute,
)
from stellar_etl_airflow.build_delete_data_for_reset_task import build_delete_data_task
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

dag = DAG(
    "testnet_data_reset",
    default_args=get_default_dag_args(),
    description="This DAG runs after the Testnet data reset that occurs periodically.",
    start_date=datetime(2023, 1, 1, 0, 0),
    schedule_interval="10 9 * * *",
    is_paused_upon_creation=literal_eval(Variable.get("use_testnet")),
    params={
        "alias": "testnet-reset",
    },
    user_defined_filters={"fromjson": lambda s: loads(s)},
    sla_miss_callback=alert_sla_miss,
)

internal_project = "test-hubble-319619"
internal_dataset = Variable.get("bq_dataset")
public_project = "test-hubble-319619"
public_dataset = Variable.get("public_dataset")
table_names = Variable.get("table_ids", deserialize_json=True)

check_run_date = build_check_execution_date(
    dag, "check_run_date", "start_reset", "end_of_execution"
)

end_of_execution = path_to_execute(dag, "end_of_execution")

start_reset = path_to_execute(dag, "start_reset")


delete_internal_accounts = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["accounts"],
    "internal",
)

delete_internal_assets = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["assets"],
    "internal",
)

delete_internal_claimable_balances = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["claimable_balances"],
    "internal",
)

delete_internal_effects = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["effects"],
    "internal",
)

delete_internal_ledgers = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["ledgers"],
    "internal",
)

delete_internal_liquidity_pools = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["liquidity_pools"],
    "internal",
)

delete_internal_offers = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["offers"],
    "internal",
)

delete_internal_operations = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["operations"],
    "internal",
)

delete_internal_signers = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["signers"],
    "internal",
)

delete_internal_trades = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["trades"],
    "internal",
)

delete_internal_transactions = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["transactions"],
    "internal",
)

delete_internal_trustlines = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    table_names["trustlines"],
    "internal",
)

delete_internal_enriched = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    "enriched_history_operations",
    "internal",
)

delete_internal_enriched_meaningful = build_delete_data_task(
    dag,
    internal_project,
    internal_dataset,
    "enriched_meaningful_history_operations",
    "internal",
)

delete_public_accounts = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["accounts"],
    "public",
)

delete_public_assets = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["assets"],
    "public",
)

delete_public_claimable_balances = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["claimable_balances"],
    "public",
)

delete_public_effects = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["effects"],
    "public",
)

delete_public_ledgers = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["ledgers"],
    "public",
)

delete_public_liquidity_pools = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["liquidity_pools"],
    "public",
)

delete_public_offers = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["offers"],
    "public",
)

delete_public_operations = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["operations"],
    "public",
)

delete_public_signers = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["signers"],
    "public",
)

delete_public_trades = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["trades"],
    "public",
)

delete_public_transactions = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["transactions"],
    "public",
)

delete_public_trustlines = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["trustlines"],
    "public",
)

delete_public_enriched = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    "enriched_history_operations",
    "public",
)


check_run_date >> [start_reset, end_of_execution]

start_reset >> [
    delete_internal_accounts,
    delete_internal_assets,
    delete_internal_claimable_balances,
    delete_internal_effects,
    delete_internal_ledgers,
    delete_internal_liquidity_pools,
    delete_internal_offers,
    delete_internal_operations,
    delete_internal_signers,
    delete_internal_trades,
    delete_internal_transactions,
    delete_internal_trustlines,
    delete_internal_enriched,
    delete_internal_enriched_meaningful,
    delete_public_accounts,
    delete_public_assets,
    delete_public_claimable_balances,
    delete_public_effects,
    delete_public_ledgers,
    delete_public_liquidity_pools,
    delete_public_offers,
    delete_public_operations,
    delete_public_signers,
    delete_public_trades,
    delete_public_transactions,
    delete_public_trustlines,
    delete_public_enriched,
]
