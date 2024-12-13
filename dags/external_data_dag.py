"""
The external_data_dag DAG exports data from external sources.
It is scheduled to export information to BigQuery at regular intervals.
"""

from ast import literal_eval
from datetime import datetime
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
from stellar_etl_airflow.build_del_ins_operator import create_del_ins_task
from stellar_etl_airflow.build_internal_export_task import (
    build_export_task,
    get_airflow_metadata,
)
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

EXTERNAL_DATA_TABLE_NAMES = Variable.get("table_ids", deserialize_json=True)
EXTERNAL_DATA_PROJECT_NAME = Variable.get("bq_project")
EXTERNAL_DATA_DATASET_NAME = Variable.get("bq_dataset")
RETOOL_TABLE_NAME = EXTERNAL_DATA_TABLE_NAMES["retool_entity_data"]
RETOOL_EXPORT_TASK_ID = "export_retool_data"

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
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
    user_defined_filters={
        "fromjson": lambda s: loads(s),
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
        "literal_eval": lambda e: literal_eval(e),
    },
)


retool_export_task = build_export_task(
    dag,
    RETOOL_EXPORT_TASK_ID,
    command="export-retool",
    cmd_args=[
        "--start-time",
        "{{ subtract_data_interval(dag, data_interval_start).isoformat() }}",
        "--end-time",
        "{{ subtract_data_interval(dag, data_interval_end).isoformat() }}",
    ],
    use_gcs=True,
    env_vars={"RETOOL_API_KEY": "{{ var.value.retool_api_key }}"},
)


def get_insert_to_bq_task(
    table_name: str,
    project: str,
    dataset: str,
    export_task_id: str,
    source_object_suffix: str,
    partition: bool,
    cluster: bool,
    table_id: str,
):
    metadata = get_airflow_metadata()
    source_objects = [
        "{{ task_instance.xcom_pull(task_ids='"
        + export_task_id
        + '\')["output"] }}'
        + source_object_suffix
    ]
    task_vars = {
        "task_id": f"del_ins_{table_name}_task",
        "project": project,
        "dataset": dataset,
        "table_name": table_name,
        "export_task_id": export_task_id,
        "source_object_suffix": source_object_suffix,
        "partition": partition,
        "cluster": cluster,
        "batch_id": metadata["batch_id"],
        "batch_date": metadata["batch_date"],
        "source_objects": source_objects,
        "table_id": table_id,
    }
    insert_to_bq_task = create_del_ins_task(
        dag, task_vars, build_del_ins_from_gcs_to_bq_task
    )
    return insert_to_bq_task


retool_insert_to_bq_task = get_insert_to_bq_task(
    table_name=RETOOL_TABLE_NAME,
    project=EXTERNAL_DATA_PROJECT_NAME,
    dataset=EXTERNAL_DATA_DATASET_NAME,
    export_task_id=RETOOL_EXPORT_TASK_ID,
    source_object_suffix="",
    partition=False,
    cluster=False,
    table_id=f"{EXTERNAL_DATA_PROJECT_NAME}.{EXTERNAL_DATA_DATASET_NAME}.{RETOOL_TABLE_NAME}",
)

retool_export_task >> retool_insert_to_bq_task
