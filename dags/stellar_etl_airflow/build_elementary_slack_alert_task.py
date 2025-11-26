import logging
from datetime import timedelta

from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.default import alert_after_max_retries
from stellar_etl_airflow.utils import access_secret


def elementary_task(dag, task_name, command, cmd_args=[], resource_cfg="default"):
    namespace = conf.get("kubernetes", "NAMESPACE")

    if namespace == "composer-user-workloads":
        config_file_location = Variable.get("kube_config_location")
        in_cluster = False
    else:
        config_file_location = None
        in_cluster = True

    requests = {
        "cpu": f"{{{{ var.json.resources.{resource_cfg}.requests.cpu }}}}",
        "memory": f"{{{{ var.json.resources.{resource_cfg}.requests.memory }}}}",
    }
    if resource_cfg == "elementaryreport":
        requests["ephemeral-storage"] = (
            f"{{{{ var.json.resources.{resource_cfg}.requests.ephemeral_storage }}}}"
        )
    container_resources = k8s.V1ResourceRequirements(requests=requests)

    dbt_image = "{{ var.value.dbt_image_name }}"

    slack_secret_name = Variable.get("dbt_elementary_secret")
    secret = access_secret(slack_secret_name, "default")
    args = [
        f"{command}",
        "--slack-token",
        f"{secret}",
        "--slack-channel-name",
        "{{ var.value.dbt_slack_elementary_channel }}",
    ]

    if len(cmd_args):
        args = [*args, *cmd_args]

    logging.info(f"sh commands to run in pod: {args}")

    return KubernetesPodOperator(
        task_id=f"elementary_slack_alert_{task_name}",
        name=f"elementary_slack_alert_{task_name}",
        namespace=Variable.get("k8s_namespace"),
        env_vars={
            "DBT_USE_COLORS": "0",
            "DBT_DATASET": "{{ var.value.dbt_elementary_dataset }}",
            "DBT_TARGET": "{{ var.value.dbt_elementary_target }}",
            "DBT_MAX_BYTES_BILLED": "{{ var.value.dbt_maximum_bytes_billed }}",
            "DBT_JOB_TIMEOUT": "{{ var.value.dbt_job_execution_timeout_seconds }}",
            "DBT_THREADS": "{{ var.value.dbt_threads }}",
            "DBT_JOB_RETRIES": "{{ var.value.dbt_job_retries }}",
            "DBT_PROJECT": "{{ var.value.dbt_project }}",
            "INTERNAL_SOURCE_DB": "{{ var.value.dbt_internal_source_db }}",
            "INTERNAL_SOURCE_SCHEMA": "{{ var.value.dbt_internal_source_schema }}",
            "PUBLIC_SOURCE_DB": "{{ var.value.dbt_public_source_db }}",
            "PUBLIC_SOURCE_SCHEMA": "{{ var.value.dbt_public_source_schema }}",
            "EXECUTION_DATE": "{{ ds }}",
            "AIRFLOW_START_TIMESTAMP": "{{ ti.start_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        },
        image=dbt_image,
        cmds=["edr"],
        arguments=args,
        dag=dag,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        container_resources=container_resources,
        on_failure_callback=alert_after_max_retries,
        image_pull_policy="IfNotPresent",
        sla=timedelta(
            seconds=Variable.get("task_sla", deserialize_json=True)[
                f"elementary_{task_name}"
            ]
        ),
        trigger_rule="all_done",
        reattach_on_restart=False,
        kubernetes_conn_id="kubernetes_default",
    )
