from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "mgi_dbt_stellar_marts",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 10, 1, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 16 * * *",  # Runs at 04:00 PM UTC / 10:00 AM CST as MGI files are received around ~10:00 AM CST
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["dbt-stellar-marts"],
    # sla_miss_callback=alert_sla_miss,
)

# Wait on ingestion DAGs
wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables"
)

wait_on_partner_pipeline_dag = build_cross_deps(
    dag, "wait_on_partner_pipeline_dag", "partner_pipeline_dag"
)

# DBT models to run

mgi_task = dbt_task(dag, tag="mgi", operator="+", excluded="stellar_dbt_public")

wait_on_dbt_enriched_base_tables >> mgi_task
wait_on_partner_pipeline_dag >> mgi_task
