import base64
import logging
import os

from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes import client, config
from kubernetes.client import models as k8s
from stellar_etl_airflow.default import alert_after_max_retries


def acess_secret(secret_name, namespace):
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        config.load_incluster_config()
    else:
        config.load_kube_config()
    v1 = client.CoreV1Api()
    secret_data = v1.read_namespaced_secret(secret_name, namespace)
    secret = secret_data.data
    secret = base64.b64decode(secret["token"]).decode("utf-8")
    return secret


def elementary_task(
    dag,
    task_name,
    resource_cfg="default",
):
    secret = acess_secret("{{ var.value.elementary_secret }}", "default")

    args = [
        "monitor",
        "--slack-token",
        f"{secret}",
        "--slack-channel-name",
        "{{ var.value.slack_elementary_channel }}",
    ]
    namespace = conf.get("kubernetes", "NAMESPACE")

    if namespace == "default":
        config_file_location = Variable.get("kube_config_location")
        in_cluster = False
    else:
        config_file_location = None
        in_cluster = True

    container_resources = k8s.V1ResourceRequirements(
        requests={
            "cpu": f"{{{{ var.json.resources.{resource_cfg}.requests.cpu }}}}",
            "memory": f"{{{{ var.json.resources.{resource_cfg}.requests.memory }}}}",
        }
    )
    affinity = Variable.get("affinity", deserialize_json=True).get(resource_cfg)

    dbt_image = "{{ var.value.dbt_image_name }}"

    logging.info(f"sh commands to run in pod: {args}")

    return KubernetesPodOperator(
        task_id=f"elementary_{task_name}",
        name=f"elementary_{task_name}",
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        env_vars={
            "DBT_USE_COLORS": "0",
            "DBT_DATASET": "{{ var.value.dbt_elementary_dataset }}",
            "DBT_TARGET": "{{ var.value.dbt_target }}",
            "DBT_MAX_BYTES_BILLED": "{{ var.value.dbt_maximum_bytes_billed }}",
            "DBT_JOB_TIMEOUT": "{{ var.value.dbt_job_execution_timeout_seconds }}",
            "DBT_THREADS": "{{ var.value.dbt_threads }}",
            "DBT_JOB_RETRIES": "{{ var.value.dbt_job_retries }}",
            "DBT_PROJECT": "{{ var.value.dbt_project }}",
            "INTERNAL_SOURCE_DB": "{{ var.value.internal_source_db }}",
            "INTERNAL_SOURCE_SCHEMA": "{{ var.value.internal_source_schema }}",
            "PUBLIC_SOURCE_DB": "{{ var.value.public_source_db }}",
            "PUBLIC_SOURCE_SCHEMA": "{{ var.value.public_source_schema }}",
            "EXECUTION_DATE": "{{ ds }}",
        },
        image=dbt_image,
        cmds=["edr"],
        arguments=args,
        dag=dag,
        do_xcom_push=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        affinity=affinity,
        container_resources=container_resources,
        on_failure_callback=alert_after_max_retries,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
    )
