from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "dbt_enriched_base_tables",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 1, 30, 0, 0),
    description="This DAG runs dbt models at a half hourly cadence",
    schedule_interval="*/30 * * * *",  # Runs every 30 mins
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-enriched-base-tables"],
)

# Wait on ingestion DAGs
# NOTE: history_archive_without_captive_core is currently disabled in
# favor of history_archive_with_captive_core_combined;
# Update with ledger exporter project integration
# wait_on_cc = build_cross_deps(
#     dag, "wait_on_ledgers_txs", "history_archive_without_captive_core"
# )
wait_on_cc = build_cross_deps(
    dag, "wait_on_ledgers_txs", "history_archive_with_captive_core_combined_export"
)
wait_on_state_table = build_cross_deps(dag, "wait_on_state_table", "state_table_export")

# DBT models to run
enriched_history_operations_task = dbt_task(dag, tag="enriched_history_operations")
current_state_task = dbt_task(dag, tag="current_state")

# DAG task graph
wait_on_cc >> enriched_history_operations_task

wait_on_state_table >> current_state_task
