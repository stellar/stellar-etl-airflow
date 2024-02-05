from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

with DAG(
    "partner_pipeline_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2023, 1, 1, 0, 0),
    description="This DAG automates daily updates to partner tables in BigQuery.",
    schedule_interval="0 13 * * *",
    params={
        "alias": "partner",
    },
    render_template_as_native_obj=True,
    catchup=False,
) as dag:
    PROJECT = "{{ var.value.bq_project }}"
    DATASET = "{{ var.value.bq_dataset }}"
    BUCKET_NAME = "{{ var.value.partners_bucket }}"
    PARTNERS = Variable.get("partners_data", deserialize_json=True)
    TODAY = "{{ data_interval_end | ds_nodash }}"
    start_tables_task = EmptyOperator(task_id="start_update_task")

    for partner in PARTNERS:
        OBJECT_PREFIX = "{}/{}_{}".format(
            PARTNERS[partner]["prefix_folder"], PARTNERS[partner]["prefix_id"], TODAY
        )
        check_gcs_file = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"check_gcs_file_{partner}",
            bucket=BUCKET_NAME,
            prefix=OBJECT_PREFIX,
            dag=dag,
            poke_interval=60,
            timeout=60 * 60 * 24,
            on_failure_callback=alert_after_max_retries,
        )

        send_partner_to_bq_internal_task = GCSToBigQueryOperator(
            task_id=f"send_{partner}_to_bq_pub_task",
            bucket=BUCKET_NAME,
            source_objects=['{{ ti.xcom_pull(key="return_value")[0] }}'],
            destination_project_dataset_table="{}.{}.{}".format(
                PROJECT, DATASET, PARTNERS[partner]["table"]
            ),
            skip_leading_rows=1,
            schema_fields=read_local_schema(PARTNERS[partner]["table"]),
            write_disposition="WRITE_TRUNCATE",
            dag=dag,
            retry_delay=timedelta(minutes=5),
            max_retry_delay=timedelta(minutes=30),
            on_failure_callback=alert_after_max_retries,
        )

        (start_tables_task >> check_gcs_file >> send_partner_to_bq_internal_task)
