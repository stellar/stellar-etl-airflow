from airflow.operators.python import PythonOperator


def initialize_task_vars(
    data_type,
    export_task_id,
    batch_id,
    batch_date,
    table_names,
    public_project,
    public_dataset,
    source_object_suffix="",
    source_objects=None,
):
    """
    Initialize task variables for data export and import.

    Args:
        data_type (str): Type of data (e.g., operations, trades).
        export_task_id (str): Task ID of the export task.
        batch_id (str): Batch ID.
        batch_date (str): Batch date.
        table_names (dict): Dictionary of table names.
        public_project (str): Public project name.
        public_dataset (str): Public dataset name.
        source_object_suffix (str): Suffix for source objects.
        source_objects (list): List of source objects.

    Returns:
        dict: Task variables.
    """
    table_name = table_names[data_type] if data_type in table_names else data_type
    if source_objects is None:
        source_objects = [
            "{{ task_instance.xcom_pull(task_ids='"
            + export_task_id
            + '\')["output"] }}'
            + source_object_suffix
        ]
    return {
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
    }


def create_del_ins_task(dag, task_id, task_vars, del_ins_callable):
    """
    Create a PythonOperator for delete and insert tasks.

    Args:
        dag (DAG): The DAG to which the task belongs.
        task_id (str): Task ID.
        task_vars (dict): Task variables.
        del_ins_callable (callable): The callable function to be used in the PythonOperator.

    Returns:
        PythonOperator: The created PythonOperator.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=del_ins_callable,
        op_args=[
            task_vars["project"],
            task_vars["dataset"],
            task_vars["table_name"],
            task_vars["export_task_id"],
            task_vars["source_object_suffix"],
            task_vars["partition"],
            task_vars["cluster"],
            task_vars["batch_id"],
            task_vars["batch_date"],
            task_vars["source_objects"],
        ],
        provide_context=True,
        dag=dag,
    )
