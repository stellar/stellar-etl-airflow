from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_bq_generate_avro_job_task import (
    build_bq_generate_avro_job,
)
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

dag = DAG(
    "generate_avro",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 10, 1, 1, 0),
    catchup=True,
    description="This DAG generates AVRO files from BQ tables",
    schedule_interval="0 * * * *",
    render_template_as_native_obj=True,
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
        "batch_run_date_as_directory_string": macros.batch_run_date_as_directory_string,
    },
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

# Add dummy_task so DAG generates the avro_tables loop and dependency graph correctly
dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

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
    "history_effects",
    "history_operations",
]

for table in avro_tables:
    avro_task = build_bq_generate_avro_job(
        dag=dag,
        project=public_project,
        dataset=public_dataset,
        table=table,
        gcs_bucket=gcs_bucket,
    )

    dummy_task >> avro_task
    wait_on_history_table >> avro_task
    wait_on_state_table >> avro_task
