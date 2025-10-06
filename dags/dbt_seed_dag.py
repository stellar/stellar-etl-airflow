from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models import Param, Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import create_dbt_profile
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()


def dbt_seed_task(dag):
    """Create a dbt seed task - simplified version of dbt_task for seed operations"""
    
    namespace = conf.get("kubernetes", "NAMESPACE")
    config_file_location = Variable.get("kube_config_location") if namespace == "default" else None
    in_cluster = namespace != "default"

    command_parts = ["seed"]
    if "{{ params.full_refresh }}" == "true":
        command_parts.append("--full-refresh")
    
    args = [" ".join([create_dbt_profile(), "dbt"] + command_parts)]
    task_sla_seconds = Variable.get("task_sla", deserialize_json=True).get("seed", 600)

    return KubernetesPodOperator(
        task_id="dbt_seed",
        name="dbt_seed",
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        image="{{ var.value.dbt_image_name }}",
        cmds=["sh", "-c"],
        arguments=args,
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "{{ var.json.resources.dbt.requests.cpu }}",
                "memory": "{{ var.json.resources.dbt.requests.memory }}",
            }
        ),
        dag=dag,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
        sla=timedelta(seconds=task_sla_seconds),
        reattach_on_restart=False,
    )

with DAG(
    "dbt_seed",
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 9, 29, 0, 0),
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
            description="Run seed with --full-refresh flag"
        )
    }
) as dag:

    # DBT seed task
    seed_task = dbt_seed_task(dag)

    seed_task
