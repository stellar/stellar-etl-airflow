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
    "dbt_stellar_marts_pricing_data_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 10, 17, 13, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 13 * * *",  # Runs at 13:00 UTC as pricing data is ingested around ~13:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["dbt-stellar-marts"],
    # sla_miss_callback=alert_sla_miss,
)

# Wait on Snapshot DAGs
wait_on_dbt_snapshot_pricing_tables = build_cross_deps(
    dag,
    "wait_on_dbt_snapshot_pricing_tables",
    "dbt_snapshot_pricing_data",
)

asset_prices_task = dbt_task(dag, tag="asset_prices")

wait_on_dbt_snapshot_pricing_tables >> asset_prices_task
