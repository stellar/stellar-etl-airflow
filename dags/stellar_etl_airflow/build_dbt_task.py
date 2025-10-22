import logging
import json
from datetime import timedelta

from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.default import alert_after_max_retries
from stellar_etl_airflow.utils import skip_retry_dbt_errors

# Define valid Airflow date macros
VALID_DATE_MACROS = {
    "ts": "{{ ts }}",
    "ds": "{{ ds }}",
    "data_interval_start": "{{ data_interval_start }}",
    "data_interval_end": "{{ data_interval_end }}",
}


def create_dbt_profile(project="prod"):
    dbt_target = "{{ var.value.dbt_target }}"
    dbt_dataset = "{{ var.value.dbt_dataset_for_test }}"
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
    operator="",
    command_type="build",
    excluded=None,
    resource_cfg="default",
    run_singular_test="false",
    run_recency_test="false",
    env_vars={},
    date_macro="ts",
):
    """Create a task to run a collection of dbt models. Models are orchestrated by tag.
    If no tag is provided, the model_name will be used. If both are provided, the tag will
    be used to select the models to run.
    If multiple models are selected, the task name will be 'multiple_models'.

    args:
        dag: parent dag for this task
        model_name: dbt model_name to run, optional
        tag: dbt tag to run, optional
        flag: dbt node selection syntax, defaults to "select"
        command_type: dbt command name, defaults to "build"
        excluded: dbt node selection syntax for excluded models, stellar-dbt and stellar-dbt-public models should not execute in one node
        resource_cfg: the resource config name defined in the airflow 'resources' variable for k8s
        run_singular_test: if true, the task will run singular tests, defaults to "false"
        run_recency_test: if true, the task will run recency tests, defaults to "false"
        date_macro: which airflow execution date macro should be passed to the dbt task, defaults to "ts", sets the env var, execution_date

    returns:
        k8s pod task
    """
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

    dbt_image = "{{ var.value.dbt_image_name }}"

    # Handle seed command - doesn't use model selection
    if command_type == "seed":
        args = [command_type]
        task_name = command_type  # Use command_type as task_name to avoid duplication
    else:
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
    # --exclude selector added for necessary use cases
    # Argument should be string or list of strings
    if excluded:
        args.append("--exclude")
        if isinstance(excluded, list):
            args.extend(excluded)
        else:
            args.append(excluded)

    try:
        execution_date = VALID_DATE_MACROS[date_macro]
    except KeyError:
        raise ValueError(
            f"Invalid date_macro: {date_macro}. Must be one of: {', '.join(VALID_DATE_MACROS.keys())}"
        )

    if Variable.get("dbt_full_refresh_models", deserialize_json=True).get(task_name):
        args.append("--full-refresh")

    logging.info(f"sh commands to run in pod: {args}")

    dbt_vars = {}

    # Add recency or singular test vars
    if run_recency_test == "true":
        dbt_vars["is_recency_airflow_task"] = "true"
    if dbt_vars:
        args.extend(
            [
                "--vars",
                json.dumps(dbt_vars).replace('"', '\\"')
            ]
        )

    env_vars.update(
        {
            "DBT_USE_COLORS": "0",
            "DBT_DATASET": "{{ var.value.dbt_dataset_for_test }}",
            "DBT_TARGET": "{{ var.value.dbt_target }}",
            "DBT_MAX_BYTES_BILLED": "{{ var.value.dbt_maximum_bytes_billed }}",
            "DBT_JOB_TIMEOUT": "{{ var.value.dbt_job_execution_timeout_seconds }}",
            "DBT_THREADS": "{{ var.value.dbt_threads }}",
            "DBT_JOB_RETRIES": "{{ var.value.dbt_job_retries }}",
            "DBT_PROJECT": "{{ var.value.dbt_project }}",
            "INTERNAL_SOURCE_DB": "{{ var.value.dbt_internal_source_db }}",
            "INTERNAL_SOURCE_SCHEMA": "{{ var.value.dbt_internal_source_schema }}",
            "PUBLIC_SOURCE_DB": "{{ var.value.dbt_public_source_db }}",
            "PUBLIC_SOURCE_SCHEMA": "{{ var.value.dbt_public_source_schema }}",
            "EXECUTION_DATE": execution_date,
            "AIRFLOW_START_TIMESTAMP": "{{ ti.start_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
            "IS_SINGULAR_AIRFLOW_TASK": run_singular_test,
        }
    )

    if command_type == "seed":
        task_id = f"dbt_{command_type}"
    else:
        task_id = f"dbt_{command_type}_{task_name}"

    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id,
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        env_vars=env_vars,
        image=dbt_image,
        arguments=args,
        dag=dag,
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        container_resources=container_resources,
        on_failure_callback=alert_after_max_retries,
        on_retry_callback=skip_retry_dbt_errors,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
        sla=timedelta(
            seconds=Variable.get("task_sla", deserialize_json=True)[task_name]
        ),
        reattach_on_restart=False,
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

    execution_date = "EXECUTION_DATE=" + "{{ ts }}"

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
        is_delete_operator_pod=True,
        startup_timeout_seconds=720,
        in_cluster=in_cluster,
        config_file=config_file_location,
        container_resources=resources_requests,
        on_failure_callback=alert_after_max_retries,
        on_retry_callback=skip_retry_dbt_errors,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[k8s.V1LocalObjectReference("private-docker-auth")],
        sla=timedelta(
            seconds=Variable.get("task_sla", deserialize_json=True)[model_name]
        ),
        reattach_on_restart=False,
    )
