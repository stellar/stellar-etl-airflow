from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    get_default_dag_args,
    init_sentry,
)

init_sentry()


def get_object_key(date, table):
    prefix = f"{{{{ var.value.gcs_exported_object_prefix }}}}/metadata/{date}"
    file_key = "/".join([prefix, f"{table}.json"])
    return file_key


def generate_query(table):
    schema = read_local_schema(table)
    columns_names = [field["name"] for field in schema]
    select_fields = ",".join(columns_names)

    if TABLES[table]["is_incremental"]:
        date_column = TABLES[table]["date_column"]
        query = f"SELECT {select_fields} FROM {table} WHERE {date_column} >= '{{{{ ds }}}}' AND {date_column} < '{{{{ macros.ds_add(ds, days=1) }}}}';"
    else:
        query = f"SELECT {select_fields} FROM {table}"
    return query


with DAG(
    "airflow_metadata_ingestion_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 5, 1, 0, 0),
    description="This DAG automates daily extraction from Airflow metadata tables and load in BigQuery.",
    schedule_interval="0 0 * * *",
    params={
        "alias": "metadata",
    },
    render_template_as_native_obj=True,
    catchup=False,
) as dag:
    PROJECT = "{{ var.value.bq_project }}"
    DATASET = "{{ var.json.airflow_metadata.bq_dataset }}"
    BUCKET_NAME = "{{ var.value.gcs_exported_data_bucket_name }}"
    TABLES = Variable.get("airflow_metadata", deserialize_json=True)["tables"]
    TODAY = "{{ data_interval_end | ds_nodash }}"

    start_metadata_task = EmptyOperator(task_id="start_metadata_task")

    for table in TABLES.keys():
        query = generate_query(table)
        extract_metadata_to_gcs = PostgresToGCSOperator(
            task_id=f"extract_{table}_metadata_to_gcs",
            postgres_conn_id="airflow_db",
            sql=query,
            bucket=BUCKET_NAME,
            filename=get_object_key(TODAY, table),
            export_format="json",
            gzip=False,
            dag=dag,
            retry_delay=timedelta(minutes=5),
            max_retry_delay=timedelta(minutes=30),
            on_failure_callback=alert_after_max_retries,
        )
        if TABLES[table]["is_incremental"]:
            send_metadata_to_bg_task = GCSToBigQueryOperator(
                task_id=f"send_{table}_metadata_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[get_object_key(TODAY, table)],
                destination_project_dataset_table="{}.{}.{}".format(
                    PROJECT, DATASET, table
                ),
                skip_leading_rows=1,
                schema_fields=read_local_schema(table),
                source_format="NEWLINE_DELIMITED_JSON",
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                dag=dag,
                retry_delay=timedelta(minutes=5),
                max_retry_delay=timedelta(minutes=30),
                on_failure_callback=alert_after_max_retries,
            )
        else:
            send_metadata_to_bg_task = GCSToBigQueryOperator(
                task_id=f"send_{table}_metadata_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[get_object_key(TODAY, table)],
                destination_project_dataset_table="{}.{}.{}".format(
                    PROJECT, DATASET, table
                ),
                skip_leading_rows=1,
                schema_fields=read_local_schema(table),
                source_format="NEWLINE_DELIMITED_JSON",
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
                dag=dag,
                retry_delay=timedelta(minutes=5),
                max_retry_delay=timedelta(minutes=30),
                on_failure_callback=alert_after_max_retries,
            )

        start_metadata_task >> extract_metadata_to_gcs >> send_metadata_to_bg_task
