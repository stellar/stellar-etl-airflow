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
    "elementary_report",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 6, 25, 0, 0),
    description="This DAG creates elementary report and send it to slack",
    schedule="*/60 * * * *",  # Runs every 60 minutes
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
) as dag:

    # Trigger elementary
    elementary_alerts = elementary_task(
        dag,
        "generate_report",
        resource_cfg="elementaryreport",
        command="send-report",
        cmd_args=["--days-back", "7", "--profiles-dir", "."],
    )

    elementary_alerts
