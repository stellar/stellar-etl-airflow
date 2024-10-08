import os
from datetime import timedelta

from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow.build_bq_insert_job_task import (
    get_query_filepath,
    file_to_string,
)
from stellar_etl_airflow import macros
from stellar_etl_airflow.default import alert_after_max_retries


def build_bq_generate_avro_job(
    dag,
    project,
    dataset,
    table,
    gcs_bucket,
):
    query_path = get_query_filepath(f"generate_avro/{table}")
    query = file_to_string(query_path)
    batch_run_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"
    next_batch_run_date = (
        "{{ batch_run_date_as_datetime_string(dag, data_interval_end) }}"
    )
    uri_datetime = (
        "{{ batch_run_date_as_directory_string(dag, data_interval_start) }}"
    )
    uri = f"gs://{gcs_bucket}/avro/{table}/{uri_datetime}/*.avro"
    sql_params = {
        "project_id": project,
        "dataset_id": dataset,
        "batch_run_date": batch_run_date,
        "next_batch_run_date": next_batch_run_date,
        "uri": uri,
    }
    query = query.format(**sql_params)
    configuration = {
        "query": {
            "query": query,
            "useLegacySql": False,
        }
    }

    return BigQueryInsertJobOperator(
        task_id=f"generate_avro_{table}",
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_bq_generate_avro_job.__name__
            ]
        ),
        on_failure_callback=alert_after_max_retries,
        configuration=configuration,
        sla=timedelta(
            seconds=Variable.get("task_sla", deserialize_json=True)[
                build_bq_generate_avro_job.__name__
            ]
        ),
    )
