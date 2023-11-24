"""
The partner_pipeline_dag DAG updates the partners table in Bigquey every day.
"""

import datetime
import json
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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
    start_date=datetime.datetime(2023, 1, 1, 0, 0),
    description="This DAG updates the partner tables in Bigquey every day",
    schedule_interval="20 15 * * *",
    params={
        "alias": "partner",
    },
    render_template_as_native_obj=True,
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    catchup=False,
) as dag:
    PROJECT = Variable.get("bq_project")
    DATASET = Variable.get("bq_dataset")
    BUCKET_NAME = Variable.get("partners_bucket")
    PARTNERS = Variable.get("partners_data", deserialize_json=True)
    TODAY = "{{ next_ds_nodash }}"
    QUERY = """ UPDATE {project}.{dataset}.{table}
                SET update_timestamp = CURRENT_TIMESTAMP()
                WHERE update_timestamp IS NULL
            """

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

        insert_ts_field = BigQueryInsertJobOperator(
            task_id=f"insert_ts_field_{partner}",
            project_id=PROJECT,
            on_failure_callback=alert_after_max_retries,
            configuration={
                "query": {
                    "query": QUERY.format(
                        project=PROJECT,
                        dataset=DATASET,
                        table=PARTNERS[partner]["table"],
                    ),
                    "useLegacySql": False,
                }
            },
        )
        start_tables_task >> check_gcs_file >> send_partner_to_bq_internal_task >> insert_ts_field
