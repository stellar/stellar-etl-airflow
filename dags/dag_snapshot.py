from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
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
    "dbt_snapshot",
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 3, 20, 0, 1),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 1 * * *",  # Runs at 01:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["custom_snapshot"],
    params={
        "backfill_start_date": Param(type="date"),
        "backfill_end_date": Param(type="date"),
    },
    # sla_miss_callback=alert_sla_miss,
)

trustline_snapshot_task = dbt_task(
    dag, tag="custom_snapshot", excluded="stellar_dbt_public"
)
trustline_snapshot_task
