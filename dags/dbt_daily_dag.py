"""
The `dbt_daily` DAG runs `dbt build` against all models except by:
    - the `enriched_history_operations` model
    - and all models tagged `ohlc`
The function dbt_task() has `build` as the default argument for `dbt`
and the graph operator needs to be an empty string to override the default `+`.
The final command is similar to: `dbt build --exclude enriched_history_operations,tag:ohlc`
"""
from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args

dag = DAG(
    "dbt_daily",
    default_args=get_default_dag_args(),
    schedule_interval=None,  # don’t schedule since this DAG is externally triggered daily
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt"],
)

dbt_daily_task = dbt_task(
    dag,
    flag="exclude",
    operator="",
    model_name="enriched_history_operations",
    tag="ohlc",
)
