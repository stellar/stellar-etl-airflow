"""
The daily_euro_ohlc_dag DAG updates the currency table in Bigquey every day.
"""

import datetime
import json
import logging

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from google.cloud import storage
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import alert_after_max_retries


@dag(
    start_date=datetime.datetime(2023, 1, 1, 0, 0),
    description="This DAG updates the currency tables in Bigquey every day",
    schedule_interval="35 0 * * *",
    params={
        "alias": "euro",
    },
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    catchup=False,
)
def daily_euro_ohlc_dag_1():
    currency_ohlc = Variable.get("currency_ohlc", deserialize_json=True)
    columns = Variable.get("columns_ohlc_currency")
    project_name = Variable.get("bq_project")
    dataset_name = Variable.get("bq_dataset")
    bucket_name = Variable.get("currency_bucket")
    TODAY = "{{ ds }}"
    FILENAME = f"{currency_ohlc['file']}-{TODAY}.csv"

    @task
    def get_daily_ohlc(endpoint, file_name):
        response = requests.get(endpoint)
        df = pd.DataFrame(response, columns=columns, index=None)
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df = df.to_csv(file_name, index=False)
        return df

    @task
    def response_to_gcs(bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(source_file_name)
        logging.info(
            f"File {source_file_name}.csv uploaded to {destination_blob_name}."
        )

    @task
    def upload_to_bq(file, bucket_name, project_name, dataset_name, table_name):
        schema_fields = read_local_schema(file)
        return GCSToBigQueryOperator(
            task_id=f"send_{file}_to_bq",
            bucket=bucket_name,
            schema_fields=schema_fields,
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=f"{FILENAME}-{TODAY}.csv",
            destination_project_dataset_table=f"{project_name}.{dataset_name}.{table_name}",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            max_bad_records=10,
            on_failure_callback=alert_after_max_retries,
            dag=dag,
        )

    get_ohlc = get_daily_ohlc(currency_ohlc["endpoint"], FILENAME)
    upload_to_gcs = response_to_gcs(bucket_name, get_ohlc, FILENAME)
    gcs_to_bq = upload_to_bq(
        currency_ohlc["file"],
        bucket_name,
        project_name,
        dataset_name,
        currency_ohlc["table_name"],
    )

    get_ohlc >> upload_to_gcs >> gcs_to_bq


daily_euro_ohlc_dag_1()
