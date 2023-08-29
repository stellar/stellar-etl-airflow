"""
The daily_euro_ohlc_dag DAG updates the currency table in Bigquey every day.
"""

import datetime
import json
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from google.cloud import storage
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import alert_after_max_retries

with DAG(
    dag_id="daily_euro_ohlc_dag",
    start_date=datetime.datetime(2023, 1, 1, 0, 0),
    description="This DAG updates the currency tables in Bigquey every day",
    schedule_interval="35 0 * * *",
    params={
        "alias": "euro",
    },
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    catchup=False,
) as dag:
    currency_ohlc = Variable.get("currency_ohlc", deserialize_json=True)
    project_name = Variable.get("bq_project")
    dataset_name = Variable.get("bq_dataset")
    bucket_name = Variable.get("currency_bucket")
    currency = currency_ohlc["currency"]
    today = "{{ ds }}"
    filename = f"{currency}-{today}.txt"

    @task()
    def response_to_gcs(bucket_name, endpoint, destination_blob_name):
        import requests

        response = requests.get(endpoint)
        euro_data = response.text
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(euro_data)
        logging.info(f"File {destination_blob_name}.txt uploaded to {bucket_name}.")

    @task()
    def upload_to_bq(file, bucket_name, project_name, dataset_name, table_name):
        schema_fields = read_local_schema(file)
        return GCSToBigQueryOperator(
            task_id=f"send_{file}_to_bq",
            bucket=bucket_name,
            schema_fields=schema_fields,
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=f"{filename}-{today}.txt",
            destination_project_dataset_table=f"{project_name}.{dataset_name}.{table_name}",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            max_bad_records=10,
            on_failure_callback=alert_after_max_retries,
            dag=dag,
        )

    upload_to_gcs = response_to_gcs(bucket_name, currency_ohlc["endpoint"], filename)
    gcs_to_bq = upload_to_bq(
        currency,
        bucket_name,
        project_name,
        dataset_name,
        currency_ohlc["table_name"],
    )

    upload_to_gcs >> gcs_to_bq
