"""
The external_data_dag DAG exports data from external sources.
It is scheduled to export information to BigQuery at regular intervals.
"""

import os
from ast import literal_eval
from datetime import datetime, timedelta
from json import loads

from airflow import DAG
from airflow.configuration import conf
from airflow.models.variable import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_del_ins_from_gcs_to_bq_task import (
    build_del_ins_from_gcs_to_bq_task,
)
from stellar_etl_airflow.build_del_ins_operator import (
    create_del_ins_task,
    initialize_task_vars,
)
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
    schedule_interval="0 22 * * *",
    params={
        "alias": "external",
    },
    render_template_as_native_obj=True,
    user_defined_macros={
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
    user_defined_filters={
        "fromjson": lambda s: loads(s),
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
        "literal_eval": lambda e: literal_eval(e),
    },
)


def stellar_etl_internal_task(
    dag, task_name, command, cmd_args=[], resource_cfg="default", output_file=""
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

    etl_cmd_string = " ".join(cmd_args)
    arguments = f""" {command} {etl_cmd_string} 1>> stdout.out 2>> stderr.out && cat stdout.out && cat stderr.out && echo "{{\\"output\\": \\"{output_file}\\"}}" >> /airflow/xcom/return.json"""

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


retool_run_id = "{{ run_id }}"
retool_filepath = os.path.join(
    Variable.get("gcs_exported_object_prefix"),
    retool_run_id,
    "retool-exported-entity.txt",
)

retool_table_name = "retool_entity_data"
retool_table_id = "test-hubble-319619.test_crypto_stellar_internal.retool_entity_data"
retool_public_project = "test-hubble-319619"
retool_public_dataset = "test_crypto_stellar_internal"
retool_batch_id = macros.get_batch_id()
retool_batch_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"
retool_export_task_id = "export_retool_data"
retool_source_object_suffix = ""
retool_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + retool_export_task_id
    + '\')["output"] }}'
    + retool_source_object_suffix
]
retool_batch_insert_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

retool_start_time = "{{ subtract_data_interval(dag, data_interval_start).isoformat() }}"
retool_end_time = "{{ subtract_data_interval(dag, data_interval_end).isoformat() }}"

retool_export_task = stellar_etl_internal_task(
    dag,
    "export_retool_data",
    "export-retool",
    cmd_args=[
        "--start-time",
        retool_start_time,
        "--end-time",
        retool_end_time,
        "--cloud-storage-bucket",
        Variable.get("gcs_exported_data_bucket_name"),
        "--cloud-provider",
        "gcp",
        "--output",
        retool_filepath,
        "-u",
        f"'batch_id={retool_batch_id},batch_run_date={retool_batch_date},batch_insert_ts={retool_batch_insert_ts}'",
    ],
    output_file=retool_filepath,
)

retool_task_vars = {
    "task_id": f"del_ins_{retool_table_name}_task",
    "project": retool_public_project,
    "dataset": retool_public_dataset,
    "table_name": retool_table_name,
    "export_task_id": "export_retool_data",
    "source_object_suffix": retool_source_object_suffix,
    "partition": False,
    "cluster": False,
    "batch_id": retool_batch_id,
    "batch_date": retool_batch_date,
    "source_objects": retool_source_objects,
    "table_id": retool_table_id,
}

retool_insert_to_bq_task = create_del_ins_task(
    dag, retool_task_vars, build_del_ins_from_gcs_to_bq_task
)

retool_export_task >> retool_insert_to_bq_task
