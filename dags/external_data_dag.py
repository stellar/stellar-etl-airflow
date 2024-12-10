"""
The external_data_dag DAG exports data from external sources.
It is scheduled to export information to BigQuery at regular intervals.
"""

from ast import literal_eval
from datetime import datetime, timedelta
from json import loads

from airflow import DAG
from airflow.configuration import conf
from airflow.models.variable import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

# Initialize the DAG
dag = DAG(
    "external_data_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 12, 5, 14, 30),
    description="This DAG exports data from external sources such as retool.",
    schedule_interval="*/10 * * * *",
    render_template_as_native_obj=True,
    user_defined_filters={
        "fromjson": lambda s: loads(s),
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
        "literal_eval": lambda e: literal_eval(e),
    },
)


def stellar_etl_internal_task(
    dag, task_name, command, cmd_args=[], resource_cfg="default"
):
    namespace = conf.get("kubernetes", "NAMESPACE")

    if namespace == "default":
        config_file_location = Variable.get("kube_config_location")
        in_cluster = False
    else:
        config_file_location = None
        in_cluster = True

    requests = {
        "cpu": f"{{{{ var.json.resources.{resource_cfg}.requests.cpu }}}}",
        "memory": f"{{{{ var.json.resources.{resource_cfg}.requests.memory }}}}",
    }
    container_resources = k8s.V1ResourceRequirements(requests=requests)

    image = "{{ var.value.stellar_etl_internal_image_name }}"

    return KubernetesPodOperator(
        task_id=task_name,
        name=task_name,
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        env_vars={
            "EXECUTION_DATE": "{{ ds }}",
            "AIRFLOW_START_TIMESTAMP": "{{ ti.start_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "RETOOL_API_KEY": "{{ var.value.retool_api_key }}",
        },
        image=image,
        cmds=[command],
        arguments=cmd_args,
        dag=dag,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        container_resources=container_resources,
        on_failure_callback=alert_after_max_retries,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
        sla=timedelta(
            seconds=Variable.get("task_sla", deserialize_json=True)[task_name]
        ),
        trigger_rule="all_done",
    )


retool_export_task = stellar_etl_internal_task(
    dag,
    "export_retool_data",
    "export-retool",
    cmd_args=[
        "--start-time",
        "2024-01-01T16:30:00+00:00",
        "--end-time",
        "2025-01-01T16:30:00+00:00",
    ],
)
