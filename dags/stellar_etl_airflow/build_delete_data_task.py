from datetime import timedelta

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import alert_after_max_retries


def build_delete_data_task(dag, project, dataset, table):
    if dataset == Variable.get("public_dataset"):
        dataset_type = "pub"
    elif dataset == Variable.get("public_dataset_new"):
        dataset_type = "pub_new"
    else:
        dataset_type = "bq"
    batch_id = macros.get_batch_id()
    batch_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"

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
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_delete_data_task.__name__
            ]
        ),
        on_failure_callback=alert_after_max_retries,
        configuration={
            "query": {
                "query": DELETE_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
    )
