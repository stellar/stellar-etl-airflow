"""
The daily_euro_ohlc_dag DAG updates the currency table in Bigquey every day.
"""

from datetime import datetime
from json import loads

from airflow import DAG
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.build_coingecko_api_to_gcs_task import response_to_gcs
from stellar_etl_airflow.default import alert_after_max_retries, alert_sla_miss

with DAG(
    dag_id="daily_euro_ohlc_dag",
    start_date=datetime(2023, 1, 1, 0, 0),
    description="This DAG updates the currency tables in Bigquey every day",
    schedule_interval="35 0 * * *",
    params={
        "alias": "euro",
    },
    user_defined_filters={"fromjson": lambda s: loads(s)},
    catchup=False,
    sla_miss_callback=alert_sla_miss,
) as dag:
    currency_ohlc = Variable.get("currency_ohlc", deserialize_json=True)
    project_name = Variable.get("bq_project")
    dataset_name = Variable.get("bq_dataset")
    bucket_name = Variable.get("currency_bucket")
    columns = currency_ohlc["columns_ohlc_currency"]
    currency = currency_ohlc["currency"]
    today = "{{ ds }}"
    filename = f"{currency}_{today}.csv"

    upload_to_gcs = PythonOperator(
        task_id=f"upload_{currency}_to_gcs",
        python_callable=response_to_gcs,
        op_kwargs={
            "bucket_name": bucket_name,
            "endpoint": currency_ohlc["endpoint"],
            "destination_blob_name": filename,
            "columns": columns,
        },
        dag=dag,
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id=f"send_{currency}_to_bq",
        bucket=bucket_name,
        schema_fields=read_local_schema(currency),
        autodetect=False,
        source_format="CSV",
        source_objects=filename,
        destination_project_dataset_table="{}.{}.{}".format(
            project_name, dataset_name, currency_ohlc["table_name"]
        ),
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        max_bad_records=10,
        on_failure_callback=alert_after_max_retries,
        dag=dag,
    )

    upload_to_gcs >> gcs_to_bq
