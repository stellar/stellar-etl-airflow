"""
This file contains functions for creating Airflow tasks to run stellar-etl-internal export functions.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.models.variable import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import alert_after_max_retries


def get_airflow_metadata():
    return {
        "batch_insert_ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "batch_date": "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}",
        "batch_id": macros.get_batch_id(),
        "run_id": "{{ run_id }}",
    }


def build_export_task(
    dag,
    task_name,
    command,
    cmd_args=[],
    env_vars={},
    use_gcs=False,
    resource_cfg="default",
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

    output_filepath = ""
    if use_gcs:
        metadata = get_airflow_metadata()
        batch_insert_ts = metadata["batch_insert_ts"]
        batch_date = metadata["batch_date"]
        batch_id = metadata["batch_id"]
        run_id = metadata["run_id"]

        output_filepath = os.path.join(
            Variable.get("gcs_exported_object_prefix"),
            run_id,
            f"{task_name}-exported-entity.txt",
        )

        cmd_args = cmd_args + [
            "--cloud-storage-bucket",
            Variable.get("gcs_exported_data_bucket_name"),
            "--cloud-provider",
            "gcp",
            "--output",
            output_filepath,
            "-u",
            f"'batch_id={batch_id},batch_run_date={batch_date},batch_insert_ts={batch_insert_ts}'",
        ]
    etl_cmd_string = " ".join(cmd_args)
    arguments = f""" {command} {etl_cmd_string} && echo "{{\\"output\\": \\"{output_filepath}\\"}}" >> /airflow/xcom/return.json"""
    env_vars.update(
        {
            "EXECUTION_DATE": "{{ ds }}",
            "AIRFLOW_START_TIMESTAMP": "{{ ti.start_date.strftime('%Y-%m-%dT%H:%M:%SZ') }}",
        }
    )

    return KubernetesPodOperator(
        task_id=task_name,
        name=task_name,
        namespace=Variable.get("k8s_namespace"),
        service_account_name=Variable.get("k8s_service_account"),
        env_vars=env_vars,
        image=image,
        cmds=["bash", "-c"],
        arguments=[arguments],
        do_xcom_push=True,
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
