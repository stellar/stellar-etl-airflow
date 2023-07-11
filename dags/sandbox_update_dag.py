"""
This DAG update the Canvas sandbox dataset with transactions tables, state tables with history once a month.
"""
import datetime
import json

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow.build_bq_insert_job_task import (
    file_to_string,
    get_query_filepath,
)
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

with DAG(
    "sandbox_update_dag",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 1, 1),
    description="This DAG updates a sandbox",
    schedule_interval="0 6 1 * *",
    params={"alias": "sandbox_dataset"},
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    catchup=False,
) as dag:
    TABLES_ID = Variable.get("table_ids", deserialize_json=True)
    PROJECT = Variable.get("bq_project")
    BQ_DATASET = Variable.get("bq_dataset")
    SANDBOX_DATASET = Variable.get("sandbox_dataset")

    start_tables_task = EmptyOperator(task_id="start_tables_task")

    query_path = get_query_filepath("update_table")
    query = file_to_string(query_path)
    for table_id in TABLES_ID:
        sql_params = {
            "project_id": PROJECT,
            "dataset_id": BQ_DATASET,
            "table_id": TABLES_ID[table_id],
            "target_dataset": SANDBOX_DATASET,
        }
        query = query.format(**sql_params)
        tables_update_task = BigQueryInsertJobOperator(
            task_id=f"update_{table_id}",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
            on_failure_callback=alert_after_max_retries,
        )

        start_tables_task >> tables_update_task
