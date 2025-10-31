from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
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
    "dbt_stellar_marts_backfill",
    default_args=get_default_dag_args(),
    start_date=datetime(2015, 9, 30, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 0 1 * *",  # monthly
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["dbt-stellar-marts"],
    # sla_miss_callback=alert_sla_miss,
)

batch_start_date = "{{ dag_run.conf.get('batch_start_date', data_interval_start) }}"
batch_end_date = "{{ dag_run.conf.get('batch_end_date', data_interval_end) }}"

entity_attribution_task = dbt_task(
    dag,
    tag="entity_attribution",
    operator="+",
    excluded=["stellar_dbt_public"],
    batch_start_date=batch_start_date,
    batch_end_date=batch_end_date,
)

entity_attribution_task