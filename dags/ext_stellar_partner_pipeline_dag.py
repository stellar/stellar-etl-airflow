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
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

with DAG(
    "ext_stellar_partner_pipeline_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2023, 1, 1, 0, 0),
    description="This DAG automates monthly updates from partner data dumps in GCS to partner tables in BigQuery.",
    # TODO: Confirm that this schedule works for monthly updates
    schedule_interval='0 0 1 * *',
    params={
        "alias": "partner-data",
    },
    render_template_as_native_obj=True,
    catchup=False,
    sla_miss_callback=alert_sla_miss,
) as dag:
    PROJECT = "{{ var.value.bq_project }}"
    DATASET = "{{ var.value.bq_dataset }}"
    BUCKET_NAME = "{{ var.value.partner_addresses_bucket }}"
    PARTNERS = Variable.get("partner_addresses_data", deserialize_json=True)
    start_tables_task = EmptyOperator(task_id="start_update_task")

    for partner in PARTNERS:
        OBJECT_PREFIX = "{}/{}_{}".format(
            PARTNERS[partner]["prefix_folder"], PARTNERS[partner]["prefix_id"], PARTNERS[partner]["date_format"]
        )
        check_gcs_file = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"check_gcs_file_{PARTNERS[partner]['prefix_folder']}_{PARTNERS[partner]['prefix_id']}",
            bucket=BUCKET_NAME,
            prefix=OBJECT_PREFIX,
            dag=dag,
            poke_interval=60,
            timeout=3600,
            on_failure_callback=alert_after_max_retries,
        )

        send_partner_data_to_bq_internal_task = GCSToBigQueryOperator(
            task_id=f"send_{PARTNERS[partner]['prefix_folder']}_{PARTNERS[partner]['prefix_id']}_to_bq_pub_task",
            bucket=BUCKET_NAME,
            # This logic pulls the latest file found by the sensor (ordered alphabetically, so latest dates will be at the end)
            source_objects=['{{ ti.xcom_pull(task_ids="check_gcs_file_' + PARTNERS[partner]['prefix_folder'] + '_' + PARTNERS[partner]['prefix_id'] + '")[-1] }}'],
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

        (start_tables_task >> check_gcs_file >> send_partner_data_to_bq_internal_task)
