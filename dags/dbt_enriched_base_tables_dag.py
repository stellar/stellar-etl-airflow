from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

dag = DAG(
    "dbt_enriched_base_tables",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 6, 11, 17, 30),
    description="This DAG runs dbt models at a half hourly cadence",
    schedule_interval="*/30 * * * *",  # Runs every 30 mins
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-enriched-base-tables"],
    # sla_miss_callback=alert_sla_miss,
)

# Wait on ingestion DAGs
wait_on_history_table = build_cross_deps(
    dag, "wait_on_ledgers_txs", "history_table_export"
)
wait_on_state_table = build_cross_deps(dag, "wait_on_state_table", "state_table_export")

# DBT models to run
enriched_history_operations_task = dbt_task(
    dag, tag="enriched_history_operations", operator="+"
)
current_state_task = dbt_task(dag, tag="current_state", operator="+")

# DAG task graph
wait_on_history_table >> enriched_history_operations_task

wait_on_state_table >> current_state_task
