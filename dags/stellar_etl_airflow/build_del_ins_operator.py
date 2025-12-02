from airflow.operators.python import PythonOperator
from stellar_etl_airflow.build_del_ins_from_gcs_to_bq_task import (
    build_del_ins_from_gcs_to_bq_task,
)
from stellar_etl_airflow.build_internal_export_task import get_airflow_metadata
from stellar_etl_airflow.default import alert_after_max_retries
from airflow.models import Variable
import os


def initialize_task_vars(
    table_id,
    table_name,
    export_task_id,
    batch_id,
    batch_date,
    public_project,
    public_dataset,
    source_object_suffix="",
    source_objects=None,
):
    """
    Initialize task variables for data export and import.

    Args:
        table_id (str): The ID of the table (json key).
        table_name (str): The name of the table (json value).
        export_task_id (str): Task ID of the export task.
        batch_id (str): Batch ID.
        batch_date (str): Batch date.
        public_project (str): Public project name.
        public_dataset (str): Public dataset name.
        source_object_suffix (str): Suffix for source objects.
        source_objects (list): List of source objects.

    Returns:
        dict: Task variables.
    """
    if source_objects is None:
        run_id = "{{ run_id }}"
        filepath = os.path.join(Variable.get("gcs_exported_object_prefix"), run_id)
        source_objects = [filepath + source_object_suffix]
    task_id = f"del_ins_{table_name}_task"
    return {
        "task_id": task_id,
        "project": public_project,
        "dataset": public_dataset,
        "table_name": table_name,
        "export_task_id": export_task_id,
        "source_object_suffix": source_object_suffix,
        "partition": True,
        "cluster": True,
        "batch_id": batch_id,
        "batch_date": batch_date,
        "source_objects": source_objects,
        "table_id": table_id,
    }


def create_del_ins_task(dag, task_vars, del_ins_callable):
    """
    Create a PythonOperator for delete and insert tasks.

    Args:
        dag (DAG): The DAG to which the task belongs.
        task_vars (dict): Task variables.
        del_ins_callable (callable): The callable function to be used in the PythonOperator.

    Returns:
        PythonOperator: The created PythonOperator.
    """
    return PythonOperator(
        task_id=task_vars["task_id"],
        python_callable=del_ins_callable,
        op_kwargs=task_vars,
        provide_context=True,
        on_failure_callback=alert_after_max_retries,
        dag=dag,
    )


# TODO: This function is not used. Delete it.
def create_export_del_insert_operator(
    dag,
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
    run_id = "{{ run_id }}"
    filepath = os.path.join(Variable.get("gcs_exported_object_prefix"), run_id)
    source_objects = [filepath + source_object_suffix]
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
