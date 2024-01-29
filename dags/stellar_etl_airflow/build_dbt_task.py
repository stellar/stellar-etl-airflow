import logging
from datetime import timedelta

from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from stellar_etl_airflow.default import alert_after_max_retries


def create_dbt_profile(project="prod"):
    dbt_target = "{{ var.value.dbt_target }}"
    dbt_dataset = "{{ var.value.dbt_dataset }}"
    dbt_maximum_bytes_billed = "{{ var.value.dbt_maximum_bytes_billed }}"
    dbt_job_execution_timeout_seconds = (
        "{{ var.value.dbt_job_execution_timeout_seconds }}"
    )
    dbt_job_retries = "{{ var.value.dbt_job_retries }}"
    dbt_project = "{{ var.value.dbt_project }}"
    dbt_threads = "{{ var.value.dbt_threads }}"
    if project == "pub":
        dbt_project = "{{ var.value.public_project }}"
        dbt_dataset = "{{ var.value.public_dataset }}"

    profiles_yml = f"""
stellar_dbt:
  target: {dbt_target}
  outputs:
    {dbt_target}:
      dataset: {dbt_dataset}
      maximum_bytes_billed: {dbt_maximum_bytes_billed}
      job_execution_timeout_seconds: {dbt_job_execution_timeout_seconds}
      job_retries: {dbt_job_retries}
      location: us
      method: oauth
      project: "{dbt_project}"
      threads: {dbt_threads}
      type: bigquery
elementary:
  outputs:
    default:
      dataset: elementary
      maximum_bytes_billed: {dbt_maximum_bytes_billed}
      job_execution_timeout_seconds: {dbt_job_execution_timeout_seconds}
      job_retries: {dbt_job_retries}
      location: us
      method: oauth
      project: "{dbt_project}"
      threads: {dbt_threads}
      type: bigquery
"""

    create_dbt_profile_cmd = f"echo '{profiles_yml}' > profiles.yml;"

    return create_dbt_profile_cmd


def dbt_task(
    dag,
    model_name=None,
    tag=None,
    flag="select",
    operator="+",
    command_type="build",
    resource_cfg="default",
):
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

    args = [command_type, f"--{flag}"]

    models = []
    if tag:
        task_name = tag
        models.append(f"{operator}tag:{tag}")
    if model_name:
        task_name = model_name
        models.append(f"{operator}{model_name}")
    if len(models) > 1:
        task_name = "multiple_models"
        args.append(",".join(models))
    else:
        args.append(models[0])

    if Variable.get("dbt_full_refresh_models", deserialize_json=True).get(task_name):
        args.append("--full-refresh")

    logging.info(f"sh commands to run in pod: {args}")

    return KubernetesPodOperator(
        task_id=f"dbt_{command_type}_{task_name}",
        name=f"dbt_{command_type}_{task_name}",
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        env_vars={
            "DBT_USE_COLORS": "0",
            "DBT_DATASET": "{{ var.value.dbt_dataset }}",
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
        },
        image=dbt_image,
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


def build_dbt_task(
    dag, model_name, command_type="run", resource_cfg="default", project="prod"
):
    """Create a task to run dbt on a selected model.

    args:
        dag: parent dag for this task
        model_name: dbt model_name to run
        command_type: dbt command name, defaults to "run"
        resource_cfg: the resource config name defined in the airflow 'resources' variable for k8s

    returns:
        k8s pod task
    """

    dbt_full_refresh = ""
    if Variable.get("dbt_full_refresh_models", deserialize_json=True).get(model_name):
        dbt_full_refresh = "--full-refresh"

    create_dbt_profile_cmd = create_dbt_profile(project)

    execution_date = "EXECUTION_DATE=" + "{{ ds }}"

    command = ["sh", "-c"]
    args = [
        " ".join(
            [
                create_dbt_profile_cmd,
                execution_date,
                "dbt",
                "--no-use-colors",
                command_type,
                "--select",
                model_name,
                dbt_full_refresh,
            ]
        )
    ]
    logging.info(f"sh commands to run in pod: {args}")

    namespace = conf.get("kubernetes", "NAMESPACE")
    if namespace == "default":
        config_file_location = Variable.get("kube_config_location")
        in_cluster = False
    else:
        config_file_location = None
        in_cluster = True
    resources_requests = (
        f"{{{{ var.json.resources.{resource_cfg}.requests | container_resources }}}}"
    )
    affinity = Variable.get("affinity", deserialize_json=True).get(resource_cfg)

    dbt_image = "{{ var.value.dbt_image_name }}"

    return KubernetesPodOperator(
        task_id=f"{project}_{model_name}",
        name=f"{project}_{model_name}",
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_dbt_task.__name__
            ]
        ),
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        image=dbt_image,
        cmds=command,
        arguments=args,
        dag=dag,
        do_xcom_push=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        affinity=affinity,
        container_resources=resources_requests,
        on_failure_callback=alert_after_max_retries,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
    )
