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
    default_args={**get_default_dag_args(), **{"depends_on_past": True}},
    start_date=datetime(2025, 4, 22, 0, 1),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 1 * * *",  # Runs at 01:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["custom_snapshot"],
    params={
        "snapshot_start_date": Param(
            default="2025-01-01", type="string"
        ),  # only used for manual runs
        "snapshot_end_date": Param(
            default="2025-01-02", type="string"
        ),  # only used for manual runs
        "snapshot_full_refresh": Param(
            default="false", type="string"
        ),  # only used for manual runs
    },
    # sla_miss_callback=alert_sla_miss,
)

wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables", time_delta=90
)

trustline_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_trustline",
    excluded="stellar_dbt_public",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

accounts_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_accounts",
    excluded="stellar_dbt_public",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

claimable_balances_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_claimable_balances",
    excluded="stellar_dbt_public",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

wait_on_dbt_enriched_base_tables >> trustline_snapshot_task
wait_on_dbt_enriched_base_tables >> accounts_snapshot_task
wait_on_dbt_enriched_base_tables >> claimable_balances_snapshot_task
