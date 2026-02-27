from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
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
    "dbt_omni_pdts",
    default_args=get_default_dag_args(),
    start_date=datetime(2026, 2, 25, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 13 * * *",  # Runs at 13:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["dbt-omni-pdts"],
    # sla_miss_callback=alert_sla_miss,
)

batch_start_date = "{{ dag_run.conf.get('batch_start_date', data_interval_start) }}"
batch_end_date = "{{ dag_run.conf.get('batch_end_date', data_interval_end) }}"

# We wait on all of dbt_stellar_marts instead of only the upstream omni_pdts dependencies
# because generally omni_pdts will always be dependent on gold tables. Instead of maintaining
# a separate set of dependencies for omni_pdts, we can just wait on the entire DAG to finish
wait_on_dbt_stellar_marts = build_cross_deps(
    dag,
    "wait_on_dbt_stellar_marts",
    "dbt_stellar_marts",
    timeout=10800,
)

omni_pdt_agg_task = dbt_task(
    dag, tag="omni_pdts", dbt_image="{{ var.value.dbt_image_latest }}"
)

wait_on_dbt_stellar_marts >> omni_pdt_agg_task
