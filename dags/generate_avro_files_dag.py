from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_bq_generate_avro_job_task import (
    build_bq_generate_avro_job
)
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

dag = DAG(
    "generate_avro_files",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 10, 1, 0, 0),
    description="This DAG generates AVRO files from BQ tables",
    schedule_interval="0 * * * *",  # Runs every hour
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=5,
    catchup=True,
    tags=["dbt-enriched-base-tables"],
    sla_miss_callback=alert_sla_miss,
)

public_project = "{{ var.value.public_project }}"
public_dataset = "{{ var.value.public_dataset }}"
gcs_bucket = "{{ var.value.avro_gcs_bucket }}"

# Wait on ingestion DAGs
wait_on_history_table = build_cross_deps(
    dag, "wait_on_ledgers_txs", "history_table_export"
)
wait_on_state_table = build_cross_deps(dag, "wait_on_state_table", "state_table_export")

# Generate AVRO files
avro_tables = [
    "accounts",
    "contract_data",
    "history_contract_events",
    "history_ledgers",
    "history_trades",
    "history_transactions",
    "liquidity_pools",
    "offers",
    "trust_lines",
    "ttl",
    # "history_effects",
    # "history_operations",
]

for table in avro_tables:
    task = build_bq_generate_avro_job(
        dag=dag,
        project=public_project,
        dataset=public_dataset,
        table=table,
        gcs_bucket=gcs_bucket,
    )

    wait_on_history_table >> task
    wait_on_state_table >> task
