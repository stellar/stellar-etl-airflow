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

dag = DAG(
    "dbt_data_quality_alerts",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 6, 11, 0, 0),
    description="This DAG runs dbt tests and Elementary alerts at a half-hourly cadence",
    schedule="*/15,*/45 * * * *",  # Runs every 15th minute and every 45th minute
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-data-quality", "dbt-elementary-alerts"],
    sla_miss_callback=alert_sla_miss,
)


# DBT tests to run
dbt_unit_tests = dbt_task(
    dag,
    command_type="test",
    tag="singular_test",
)
unit_tests_elementary_alerts = elementary_task(dag, "dbt_data_quality")
start_tests = EmptyOperator(task_id="start_tests_task")

# DAG task graph
start_tests >> dbt_unit_tests >> unit_tests_elementary_alerts
