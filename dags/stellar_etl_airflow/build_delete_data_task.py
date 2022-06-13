from datetime import timedelta
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow import macros

def build_delete_data_task(dag, project, dataset, table):
    if dataset == Variable.get('public_dataset'):
        dataset_type = 'pub'
    else:
        dataset_type = 'bq'
    batch_id = macros.get_batch_id()
    batch_date = '{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}'

    # Adding the partition to the filter clause prunes the query
    # if the table is partitioned (tables partitioned by batch_run_date)
    DELETE_ROWS_QUERY = (
        f"DELETE FROM {dataset}.{table} "
        f"WHERE batch_run_date = '{batch_date}'"
        f"AND batch_id = '{batch_id}';"
    )

    return BigQueryInsertJobOperator(
        project_id=project,
        task_id=f"delete_old_partition_{table}_{dataset_type}",
        execution_timeout=timedelta(seconds=Variable.get('task_timeout', deserialize_json=True)[build_delete_data_task.__name__]),
        configuration={
            "query": {
                "query": DELETE_ROWS_QUERY,
                "useLegacySql": False,
            }
        }
    )
    