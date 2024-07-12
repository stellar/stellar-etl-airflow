from airflow.operators.python import PythonOperator


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
        source_objects = [
            "{{ task_instance.xcom_pull(task_ids='"
            + export_task_id
            + '\')["output"] }}'
            + source_object_suffix
        ]
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
        dag=dag,
    )
