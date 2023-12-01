from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "enriched_tables",
    default_args=get_default_dag_args(),
    start_date=datetime(2023, 4, 12, 0, 0),
    description="This DAG runs dbt to create the tables for the models in marts/enriched/.",
    schedule_interval="*/30 * * * *",  # Runs every 30 mins
    render_template_as_native_obj=True,
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
)


# Batch loading of derived table, `enriched_history_operations` which denormalizes ledgers, transactions and operations data.
# Must wait on history_archive_without_captive_core_dag to finish before beginning the job.
wait_on_dag = build_cross_deps(
    dag, "wait_on_ledgers_txs", "history_archive_without_captive_core"
)

# tasks for staging tables
stg_history_ledgers = build_dbt_task(dag, "stg_history_ledgers")
stg_history_transactions = build_dbt_task(dag, "stg_history_transactions")
stg_history_operations = build_dbt_task(dag, "stg_history_operations")
stg_meaningful_assets = build_dbt_task(dag, "stg_meaningful_assets")
stg_sdf_accounts = build_dbt_task(dag, "stg_sdf_accounts")

# tasks for enriched tables
enriched_history_operations = build_dbt_task(dag, "enriched_history_operations")
enriched_history_operations_meaningful = build_dbt_task(
    dag, "enriched_history_operations_meaningful"
)
enriched_history_operations_xlm = build_dbt_task(dag, "enriched_history_operations_xlm")

# DAG task graph
# graph for enriched tables
wait_on_dag >> stg_history_ledgers >> enriched_history_operations
wait_on_dag >> stg_history_transactions >> enriched_history_operations
wait_on_dag >> stg_history_operations >> enriched_history_operations

enriched_history_operations >> enriched_history_operations_meaningful
stg_meaningful_assets >> enriched_history_operations_meaningful
enriched_history_operations >> enriched_history_operations_xlm
stg_sdf_accounts >> enriched_history_operations_xlm
