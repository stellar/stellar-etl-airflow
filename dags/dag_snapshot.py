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
        "backfill_start_date": Param(
            default="2025-01-01", type="string"
        ),  # only used for manual runs
        "backfill_end_date": Param(
            default="2025-01-02", type="string"
        ),  # only used for manual runs
    },
    # sla_miss_callback=alert_sla_miss,
)

trustline_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_trustline",
    excluded="stellar_dbt_public",
    env_vars={
        "BACKFILL_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.backfill_start_date }}",
        "BACKFILL_END_DATE": "{{ macros.ds_add(ds, 1) if run_id.startswith('scheduled_') else params.backfill_end_date }}",
    },
)

accounts_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_accounts",
    excluded="stellar_dbt_public",
    env_vars={
        "BACKFILL_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.backfill_start_date }}",
        "BACKFILL_END_DATE": "{{ macros.ds_add(ds, 1) if run_id.startswith('scheduled_') else params.backfill_end_date }}",
    },
)

claimable_balances_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_claimable_balances",
    excluded="stellar_dbt_public",
    env_vars={
        "BACKFILL_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.backfill_start_date }}",
        "BACKFILL_END_DATE": "{{ macros.ds_add(ds, 1) if run_id.startswith('scheduled_') else params.backfill_end_date }}",
    },
)

trustline_snapshot_task
accounts_snapshot_task
claimable_balances_snapshot_task
