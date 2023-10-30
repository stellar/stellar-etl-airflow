"""
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into BigQuery.
"""

from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from sentry_sdk import capture_message, push_scope
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import alert_after_max_retries


def export_to_lake(dag, export_task_id):
    bucket_source = Variable.get("gcs_exported_data_bucket_name")
    bucket_destination = Variable.get("ledger_transaction_data_lake_bucket_name")
    return GCSToGCSOperator(
        dag=dag,
        task_id="export_data_to_lake",
        source_bucket=bucket_source,
        source_objects=[
            "{{ task_instance.xcom_pull(task_ids='"
            + export_task_id
            + '\')["output"] }}'
        ],
        destination_bucket=bucket_destination,
        destination_object=[
            "{{ task_instance.xcom_pull(task_ids='"
            + export_task_id
            + '\')["output"][13:] }}'
        ],
        exact_match=True,
    )
