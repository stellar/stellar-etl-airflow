from datetime import datetime

from airflow import DAG
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

# This DAG should be disabled until upstream tables are corrected

dag = DAG(
    "generate_avro_daily",
    default_args=get_default_dag_args(),
    start_date=datetime(2026, 1, 12, 13, 0),
    catchup=True,
    description="This DAG generates AVRO files from BQ tables",
    schedule_interval="0 13 * * *",
    render_template_as_native_obj=True,
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
        "batch_run_date_as_directory_string": macros.batch_run_date_as_directory_string,
    },
    sla_miss_callback=alert_sla_miss,
)

public_project = "{{ var.value.public_project }}"
dbt_mart_dataset = "{{ var.value.dbt_mart_dataset }}"

internal_project = "{{ var.value.bq_project }}"
dbt_gold_dataset = "{{ var.value.dbt_gold_dataset }}"

gcs_bucket = "{{ var.value.avro_gcs_bucket }}"


# Wait on dbt DAGs
wait_on_dbt_stellar_marts = build_cross_deps(
    dag, "wait_on_dbt_stellar_marts", "dbt_stellar_marts"
)

account_activity__daily_agg_avro_task = build_bq_generate_avro_job(
    dag=dag,
    project=internal_project,
    dataset=dbt_gold_dataset,
    table="account_token_activity",
    gcs_bucket=gcs_bucket,
)

account_balances__daily_agg_agg_avro_task = build_bq_generate_avro_job(
    dag=dag,
    project=public_project,
    dataset=dbt_mart_dataset,
    table="account_balances",
    gcs_bucket=gcs_bucket,
)

asset_balances__daily_agg_avro_task = build_bq_generate_avro_job(
    dag=dag,
    project=public_project,
    dataset=dbt_mart_dataset,
    table="token_balances",
    gcs_bucket=gcs_bucket,
)

wait_on_dbt_stellar_marts >> account_activity__daily_agg_avro_task

wait_on_dbt_stellar_marts >> account_balances__daily_agg_agg_avro_task

wait_on_dbt_stellar_marts >> asset_balances__daily_agg_avro_task
