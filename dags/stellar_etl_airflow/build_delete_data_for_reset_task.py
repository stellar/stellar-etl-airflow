from datetime import timedelta

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow.default import alert_after_max_retries


def build_delete_data_task(dag, project, dataset, table, type_of_dataset):
    DELETE_ROWS_QUERY = (
        f"DELETE FROM {project}.{dataset}.{table} "
        "WHERE 1=1"
    )

    return BigQueryInsertJobOperator(
        dag=dag,
        project_id=project,
        task_id=f"delete_{table}_{type_of_dataset}",
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
