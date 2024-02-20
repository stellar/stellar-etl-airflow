from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args

dag = DAG(
    "dbt_sdf_marts",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 1, 26, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 16 * * *",  # Runs at 16:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["dbt-sdf-marts"],
)

tags = Variable.get("dbt_tags", deserialize_json=True)
tags = tags["sdf_marts"]

# Wait on ingestion DAGs
wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables"
)

wait_on_partner_pipeline_dag = build_cross_deps(
    dag, "wait_on_partner_pipeline_dag", "partner_pipeline_dag"
)

# DBT models to run
sdf_marts = dbt_task(dag, tag=tags)


# DAG task graph
wait_on_dbt_enriched_base_tables >> sdf_marts

wait_on_partner_pipeline_dag >> sdf_marts
