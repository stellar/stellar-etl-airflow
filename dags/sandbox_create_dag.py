"""
This DAG creates the sandbox dataset with transactions tables, state tables with history and views.
"""

from datetime import timedelta
from json import loads

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_bq_insert_job_task import (
    file_to_string,
    get_query_filepath,
)
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

with DAG(
    "sandbox_create_dag",
    default_args=get_default_dag_args(),
    description="This DAG creates a sandbox",
    schedule_interval="@once",
    params={"alias": "sandbox_dataset"},
    user_defined_filters={
        "fromjson": lambda s: loads(s),
    },
    catchup=False,
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
    sla_miss_callback=alert_sla_miss,
) as dag:
    PROJECT = Variable.get("public_project")
    DATASET = Variable.get("public_dataset")
    SANDBOX_PROJECT = Variable.get("bq_project")
    SANDBOX_DATASET = Variable.get("sandbox_dataset")
    DBT_DATASET = Variable.get("dbt_mart_dataset")
    TABLES_ID = Variable.get("table_ids", deserialize_json=True)
    DBT_TABLES = Variable.get("dbt_tables", deserialize_json=True)

    batch_run_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"

    start_tables_task = EmptyOperator(task_id="start_tables_task")
    start_views_task = EmptyOperator(task_id="start_views_task")

    for table_id in TABLES_ID:
        query_path = get_query_filepath("create_table")
        query = file_to_string(query_path)
        sql_params = {
            "project_id": PROJECT,
            "dataset_id": DATASET,
            "table_id": TABLES_ID[table_id],
            "target_project": SANDBOX_PROJECT,
            "target_dataset": SANDBOX_DATASET,
            "batch_run_date": batch_run_date,
        }
        query = query.format(**sql_params)
        tables_create_task = BigQueryInsertJobOperator(
            task_id=f"create_{table_id}",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
            on_failure_callback=alert_after_max_retries,
            sla=timedelta(
                seconds=Variable.get("task_sla", deserialize_json=True)[
                    "create_sandbox"
                ]
            ),
        )

        start_tables_task >> tables_create_task

    for dbt_table in DBT_TABLES:
        query_path = get_query_filepath("create_view")
        query = file_to_string(query_path)
        sql_params = {
            "project_id": PROJECT,
            "dataset_id": DBT_DATASET,
            "table_id": DBT_TABLES[dbt_table],
            "target_project": SANDBOX_PROJECT,
            "target_dataset": SANDBOX_DATASET,
        }
        query = query.format(**sql_params)
        dbt_tables_create_task = BigQueryInsertJobOperator(
            task_id=f"create_{dbt_table}",
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
            on_failure_callback=alert_after_max_retries,
            sla=timedelta(
                seconds=Variable.get("task_sla", deserialize_json=True)[
                    "create_sandbox"
                ]
            ),
        )
        start_views_task >> dbt_tables_create_task
