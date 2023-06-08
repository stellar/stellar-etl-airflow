from datetime import timedelta

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import alert_after_max_retries


def build_copy_table(
    dag,
    source_project,
    source_dataset,
    source_table,
    destination_project,
    destination_dataset,
    destination_table,
    write_disposition="WRITE_APPEND",
):
    """Create a task to copy a table.

    args:
        dag: parent dag for this task
        source_project: table to copy's project
        source_dataset: table to copy's dataset
        source_table: table to copy
        destination_project: destination project
        destination_dataset: destination dataset
        destination_table: destination table
        write_disposition: specifies the action that occurs
            if the table exists, defaults to "WRITE_APPEND"

    returns:
        BigQueryInsertJobOperator
    """

    configuration = {
        "copy": {
            "sourceTable": {
                "projectId": source_project,
                "datasetId": source_dataset,
                "tableId": source_table,
            },
            "destinationTable": {
                "projectId": destination_project,
                "datasetId": destination_dataset,
                "tableId": destination_table,
            },
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": write_disposition,
        }
    }

    return BigQueryInsertJobOperator(
        task_id=f"copy_table_{source_table}",
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_copy_table.__name__
            ]
        ),
        on_failure_callback=alert_after_max_retries,
        configuration=configuration,
        job_type="COPY",
    )
