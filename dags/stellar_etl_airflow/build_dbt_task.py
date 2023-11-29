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
                "elementary",
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
    if project == "pub":
        dbt_image = "{{ var.value.public_dbt_image_name }}"

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
        image_pull_policy="Always",  # TODO: Update to ifNotPresent when image pull issue is fixed
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
    )
