from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.build_elementary_slack_alert_task import elementary_task
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

with DAG(
    "dbt_source_data_freshness_tests",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 9, 24, 0, 0),
    description="This DAG runs source freshness tests at 30 min cadence",
    schedule="*/30 * * * *",  # Every day at 3:00 PM UTC / 9:00 AM CST
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-data-quality"],
    # sla_miss_callback=alert_sla_miss,
) as dag:

    # DBT tests to run
    source_freshness_tests = dbt_task(
        dag,
        command_type="source",
        sub_command="freshness",
        cmd_args=[
            "source:{{ var.value.dbt_internal_source_schema }}.*",
            "source:{{ var.value.dbt_public_source_db }}.*",
        ],
        flag=None,
        tag=None,
        resource_cfg="dbt",
    )
