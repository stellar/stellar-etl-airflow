"""
This file contains functions for creating Airflow tasks to load csv files from Google Cloud Storage into BigQuery.
"""

from datetime import timedelta

from airflow.macros import ds_format
from airflow.models import TaskInstance
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import alert_after_max_retries

today = "{{ next_ds_nodash }}"
object_prefix = f"stellar_transaction_extract_{today}"


def build_check_gcs_file(task_id, dag, bucket_name):
    return GCSObjectsWithPrefixExistenceSensor(
        task_id=task_id,
        bucket=bucket_name,
        prefix=object_prefix,
        dag=dag,
        poke_interval=60,
        timeout=60 * 60 * 24,
        on_failure_callback=alert_after_max_retries,
    )


def build_gcs_csv_to_bq_task(
    task_id, dag, project_id, dataset_id, table_name, bucket_name
):
    return GCSToBigQueryOperator(
        task_id=task_id,
        bucket=bucket_name,
        source_objects=['{{ ti.xcom_pull(key="return_value")[0] }}'],
        destination_project_dataset_table=f"{project_id}.{dataset_id}.{table_name}",
        skip_leading_rows=1,
        schema_fields=read_local_schema(table_name),
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
        retry_delay=timedelta(minutes=5),
        max_retry_delay=timedelta(minutes=30),
        on_failure_callback=alert_after_max_retries,
    )
