from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_elementary_slack_alert_task import elementary_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

with DAG(
    "dbt_data_quality_alerts",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 6, 25, 0, 0),
    description="This DAG runs dbt tests and Elementary alerts at a half-hourly cadence",
    schedule="*/15 * * * *",  # Runs every 15 minutes
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-data-quality", "dbt-elementary-alerts"],
    # sla_miss_callback=alert_sla_miss,
) as dag:

    # Trigger elementary
    elementary_alerts = elementary_task(
        dag,
        "dbt_data_quality",
        "monitor",
        resource_cfg="dbt",
        cmd_args=[
            "--profiles-dir",
            ".",
        ],
    )

    elementary_alerts
