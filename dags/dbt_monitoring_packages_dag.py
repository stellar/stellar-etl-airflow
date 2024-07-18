from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

with DAG(
    "dbt_monitoring_packages",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 7, 3, 0, 0),
    description="This DAG runs dbt build related to data observability and monitoring tag",
    schedule="0 8 * * *",
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["monitoring"],
) as dag:

    # DBT tests to run
    monitoring_task = dbt_task(
        dag,
        tag="monitoring_packages",
    )
    start_task = EmptyOperator(task_id="start_monitoring_task")

    # DAG task graph
    start_task >> monitoring_task
