"""
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into BigQuery.
"""

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.build_gcs_to_bq_task import CustomGCSToBigQueryOperator
from stellar_etl_airflow.default import alert_after_max_retries, init_sentry

init_sentry()


def build_data_lake_to_bq_task(
    dag, export_task_id, project, dataset, data_type, ledger_range
):
    source_objects = []
    for ledger in range(ledger_range["start"], ledger_range["end"]):
        source_objects.append(f"{ledger}.txt")
    bucket_name = Variable.get("ledger_transaction_data_lake_bucket_name")
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
    return GoogleCloudStorageToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket=bucket_name,
        source_objects=source_objects,
        destination_project_dataset_table=f"{project}.{dataset}.{data_type}",
        schema_fields=schema_fields,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        time_partitioning=time_partition,
        on_failure_callback=alert_after_max_retries,
    )
