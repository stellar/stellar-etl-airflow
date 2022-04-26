from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow import macros

def build_delete_data_task(dag, table):
    PROJECT_ID = Variable.get('bq_project')
    DATASET_ID = Variable.get('bq_dataset')
    batch_id = macros.get_batch_id()
    batch_date = '{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}'

    # Adding the partition to the filter clause prunes the query
    # if the table is partitioned (tables partitioned by batch_run_date)
    DELETE_ROWS_QUERY = (
        f"DELETE FROM {DATASET_ID}.{table} "
        f"WHERE batch_run_date = '{batch_date}'"
        f"AND batch_id = '{batch_id}';"
    )

    return BigQueryInsertJobOperator(
        project_id=PROJECT_ID,
        task_id=f"delete_old_partition_{table}",
        configuration={
            "query": {
                "query": DELETE_ROWS_QUERY,
                "useLegacySql": False,
            }
        }
    )
    