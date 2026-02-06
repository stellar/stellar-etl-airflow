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
    start_date=datetime(2026, 1, 1, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="@weekly",  # Runs weekly
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["dbt-stellar-marts"],
    # sla_miss_callback=alert_sla_miss,
)

# batch_start_date = "{{ dag_run.conf.get('batch_start_date', data_interval_start) }}"
batch_start_date = "2026-01-01"
# batch_end_date = "{{ dag_run.conf.get('batch_end_date', data_interval_end) }}"
batch_end_date = "2026-02-05"

project = "{{ var.value.bq_project }}"
dataset = "{{ var.value.dbt_internal_marts_dataset }}"

asset_balance_agg_task = dbt_task(
    dag,
    tag="asset_balance_agg",
    operator="+",
    excluded=[
        "+snapshots",
        "assets",
        "+tag:token_transfer",
        "+tag:entity_attribution",
        "+tag:wallet_metrics",
    ],
)

# Disable soroban tables because they're broken
# soroban = dbt_task(dag, tag="soroban", operator="+")
# Disable snapshot state tables because they're broken
# snapshot_state = dbt_task(dag, tag="snapshot_state")
# Disable releveant_asset_trades due to bugs in SCD tables
# relevant_asset_trades = dbt_task(dag, tag="relevant_asset_trades")

# DAG task graph
asset_balance_agg_task
