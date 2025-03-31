from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag_args = get_default_dag_args()
dag_args["retries"] = 0  # Override retries for this DAG

with DAG(
    "dbt_singular_tests",
    default_args=dag_args,
    start_date=datetime(2024, 6, 25, 0, 0),
    description="This DAG runs non-model dbt tests half-hourly cadence",
    schedule="*/30 * * * *",  # Runs every 30 minutes
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt-data-quality"],
    # sla_miss_callback=alert_sla_miss,
) as dag:

    # DBT tests to run
    singular_tests = dbt_task(
        dag,
        command_type="test",
        tag="singular_test",
        resource_cfg="dbt",
        run_singular_test="true",
    )

    singular_tests
