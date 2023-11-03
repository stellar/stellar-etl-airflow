"""
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into BigQuery.
"""

from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from sentry_sdk import capture_message, push_scope
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.build_gcs_to_bq_task import CustomGCSToBigQueryOperator
from stellar_etl_airflow.default import alert_after_max_retries, init_sentry

init_sentry()


def build_data_lake_to_bq_task(
    dag,
    export_task_id,
    project,
    dataset,
    data_type,
):
    bucket_name = Variable.get("gcs_exported_data_bucket_name")
    time_partition = {
        "field": "closed_at",
        "type": "DAY",
        "expiration": 1,
        "require_partition_filter": True,
    }
    schema_fields = read_local_schema(f"history_{data_type}")
    destination_data = [
        "{{ task_instance.xcom_pull(task_ids='"
        + export_task_id
        + '\')["output"][13:] }}'
    ]
    return CustomGCSToBigQueryOperator(
        task_id=f"send_{data_type}_to_bq",
        bucket=bucket_name,
        schema_fields=schema_fields,
        schema_update_options=["ALLOW_FIELD_ADDITION"],
        autodetect=False,
        source_format="NEWLINE_DELIMITED_JSON",
        source_objects=destination_data[0],
        destination_project_dataset_table=f"{project}.{dataset}.{data_type}",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        schema_update_option="ALLOW_FIELD_ADDITION",
        max_bad_records=0,
        time_partitioning=time_partition,
        export_task_id=export_task_id,
        on_failure_callback=alert_after_max_retries,
        dag=dag,
    )
