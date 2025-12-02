from datetime import datetime

from airflow import DAG
from airflow.models import Param
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

with DAG(
    "dbt_seed",
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 11, 26, 0, 0),
    description="This DAG runs dbt seed to materialize seed tables - manual trigger only",
    schedule_interval=None,  # Manual trigger only
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-seed", "manual-trigger"],
    params={
        "full_refresh": Param(
            default=False,
            type="boolean",
            description="Run seed with --full-refresh flag",
        )
    },
) as dag:

    seed_task = dbt_task(
        dag,
        command_type="seed",
        resource_cfg="dbt",
    )

    seed_task
