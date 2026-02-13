from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)


# TODO Write Class for grabbing last_updated file in GCS bucket
def get_latest_file_by_metadata(bucket_name: str, prefix: str, **context) -> str:
    """
    Get the most recently uploaded file based on GCS blob metadata.
    Uses the 'updated' timestamp from GCS to find the latest file.
    """
    hook = GCSHook()
    # Get the actual bucket object to access blob metadata
    bucket = hook.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        raise ValueError(
            f"No files found in bucket '{bucket_name}' with prefix: {prefix}"
        )

    # Find blob with most recent 'updated' timestamp
    latest_blob = max(blobs, key=lambda b: b.updated)
    return latest_blob.name


init_sentry()
with DAG(
    "ext_stellar_partner_pipeline_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2026, 2, 1, 0, 0),
    description="This DAG automates monthly updates from partner data dumps in GCS to partner tables in BigQuery.",
    schedule_interval="0 0 1 * *",
    params={
        "alias": "partner-data",
    },
    render_template_as_native_obj=True,
    catchup=False,
    sla_miss_callback=alert_sla_miss,
) as dag:
    PROJECT = "{{ var.value.bq_project }}"
    DATASET = "{{ var.value.bq_dataset }}"
    BUCKET_NAME = Variable.get("partner_addresses_bucket")
    PARTNERS = Variable.get("partner_addresses_data", deserialize_json=True)
    start_tables_task = EmptyOperator(task_id="start_update_task")

    for partner in PARTNERS:
        partner_config = PARTNERS[partner]
        task_id_suffix = (
            f"{partner_config['prefix_folder']}_{partner_config['prefix_id']}"
        )

        # Prefix to match files: {prefix_folder}/{prefix_id}_
        OBJECT_PREFIX = (
            f"{partner_config['prefix_folder']}/{partner_config['prefix_id']}_"
        )

        get_latest_file_task = PythonOperator(
            task_id=f"get_latest_file_{task_id_suffix}",
            python_callable=get_latest_file_by_metadata,
            op_kwargs={
                "bucket_name": BUCKET_NAME,
                "prefix": OBJECT_PREFIX,
            },
            dag=dag,
            on_failure_callback=alert_after_max_retries,
        )

        send_partner_data_to_bq_task = GCSToBigQueryOperator(
            task_id=f"send_{task_id_suffix}_to_bq_task",
            bucket=BUCKET_NAME,
            source_objects=[
                f'{{{{ ti.xcom_pull(task_ids="get_latest_file_{task_id_suffix}") }}}}'
            ],
            destination_project_dataset_table="{}.{}.{}".format(
                PROJECT, DATASET, partner_config["table"]
            ),
            skip_leading_rows=1,
            schema_fields=read_local_schema(partner_config["table"]),
            write_disposition="WRITE_TRUNCATE",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            max_retry_delay=timedelta(minutes=30),
            on_failure_callback=alert_after_max_retries,
        )

        (start_tables_task >> get_latest_file_task >> send_partner_data_to_bq_task)
