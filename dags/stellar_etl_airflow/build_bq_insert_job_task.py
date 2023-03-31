import os
from datetime import timedelta
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import alert_after_max_retries


def get_query_filepath(query_name):
    root = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(root, f"queries/{query_name}.sql")


def file_to_string(sql_path):
    """Converts a SQL file with a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    with open(sql_path, "r") as sql_file:
        return sql_file.read()


def build_bq_insert_job(
    dag, project, dataset, table, partition, cluster=False, create=False
):
    if dataset == Variable.get("public_dataset"):
        dataset_type = "pub"
    else:
        dataset_type = "bq"
    query_path = get_query_filepath(table)
    query = file_to_string(query_path)
    batch_id = macros.get_batch_id()
    batch_run_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"
    prev_batch_run_date = (
        "{{ batch_run_date_as_datetime_string(dag, prev_data_interval_start_success) }}"
    )
    next_batch_run_date = (
        "{{ batch_run_date_as_datetime_string(dag, data_interval_end) }}"
    )
    sql_params = {
        "project_id": project,
        "dataset_id": dataset,
        "batch_id": batch_id,
        "batch_run_date": batch_run_date,
        "prev_batch_run_date": prev_batch_run_date,
        "next_batch_run_date": next_batch_run_date,
    }
    query = query.format(**sql_params)
    configuration = {
        "query": {
            "query": query,
            "destinationTable": {
                "projectId": project,
                "datasetId": dataset,
                "tableId": table,
            },
            "useLegacySql": False,
            "writeDisposition": "WRITE_APPEND",
        }
    }
    if partition:
        partition_fields = Variable.get("partition_fields", deserialize_json=True)
        configuration["query"]["time_partitioning"] = partition_fields[table]
    if cluster:
        cluster_fields = Variable.get("cluster_fields", deserialize_json=True)
        configuration["query"]["clustering"] = {"fields": cluster_fields[table]}
    if create:
        configuration["query"]["createDisposition"] = "CREATE_IF_NEEDED"

    return BigQueryInsertJobOperator(
        task_id=f"insert_records_{table}_{dataset_type}",
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_bq_insert_job.__name__
            ]
        ),
        on_failure_callback=alert_after_max_retries,
        configuration=configuration,
    )
