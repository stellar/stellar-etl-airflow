"""
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into BigQuery.
"""
from datetime import timedelta

from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from sentry_sdk import capture_message, push_scope
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema
from stellar_etl_airflow.default import alert_after_max_retries


class CustomGCSToBigQueryOperator(GCSToBigQueryOperator):
    template_fields = list(GCSToBigQueryOperator.template_fields) + [
        "failed_transforms",
        "max_failed_transforms",
        "export_task_id",
    ]

    def __init__(
        self, failed_transforms, max_failed_transforms, export_task_id, **kwargs
    ):
        self.failed_transforms = failed_transforms
        self.max_failed_transforms = max_failed_transforms
        self.export_task_id = export_task_id
        super().__init__(**kwargs)

    def pre_execute(self, **kwargs):
        if int(self.failed_transforms) > self.max_failed_transforms:
            with push_scope() as scope:
                scope.set_tag("data-quality", "max-failed-transforms")
                scope.set_extra("failed_transforms", self.failed_transforms)
                scope.set_extra("file", f"gs://{self.bucket}/{self.source_objects[0]}")
                scope.set_extra("task_id", self.task_id)
                scope.set_extra("export_task_id", self.export_task_id)
                scope.set_extra(
                    "destination_table", self.destination_project_dataset_table
                )
                capture_message(
                    f"failed_transforms ({self.failed_transforms}) has exceeded the max value ({self.max_failed_transforms})",
                    "fatal",
                )
        super().pre_execute(**kwargs)


def build_gcs_to_bq_task(
    dag,
    export_task_id,
    project,
    dataset,
    data_type,
    source_object_suffix,
    partition,
    cluster,
):
    """
    Creates a task to load a file from Google Cloud Storage into BigQuery.
    The name of the file being loaded is retrieved through Airflow's Xcom.
    Data types should be: 'ledgers', 'operations', 'trades', 'transactions', or 'factEvents'.

    Parameters:
        dag - parent dag that the task will be attached to
        export_task_id - id of export task that this should consume the XCOM return value of
        source_object_suffix - string, suffix of source object path
        partition - bool if the table is partitioned
    Returns:
        the newly created task
    """

    bucket_name = Variable.get("gcs_exported_data_bucket_name")
    history_tables = [
        "ledgers",
        "assets",
        "transactions",
        "operations",
        "trades",
        "effects",
    ]
    if cluster:
        cluster_fields = Variable.get("cluster_fields", deserialize_json=True)
        cluster_fields = (
            cluster_fields[f"history_{data_type}"]
            if data_type in history_tables
            else cluster_fields[data_type]
        )
    else:
        cluster_fields = None
    project_name = project
    if dataset == Variable.get("public_dataset"):
        dataset_type = "pub"
    elif dataset == Variable.get("public_dataset_new"):
        dataset_type = "pub_new"
    else:
        dataset_type = "bq"
    dataset_name = dataset
    time_partition = {}
    if partition:
        partition_fields = Variable.get("partition_fields", deserialize_json=True)
        partition_fields = (
            partition_fields[f"history_{data_type}"]
            if data_type in history_tables
            else partition_fields[data_type]
        )
        time_partition["type"] = partition_fields["type"]
        time_partition["field"] = partition_fields["field"]
    staging_table_suffix = ""
    if data_type == "history_assets":
        staging_table_suffix = "_staging"
    if data_type in history_tables:
        schema_fields = read_local_schema(f"history_{data_type}")
        return CustomGCSToBigQueryOperator(
            task_id=f"send_{data_type}_to_{dataset_type}",
            execution_timeout=timedelta(
                seconds=Variable.get("task_timeout", deserialize_json=True)[
                    build_gcs_to_bq_task.__name__
                ]
            ),
            bucket=bucket_name,
            schema_fields=schema_fields,
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=[
                "{{ task_instance.xcom_pull(task_ids='"
                + export_task_id
                + '\')["output"] }}'
                + source_object_suffix
            ],
            destination_project_dataset_table=f"{project_name}.{dataset_name}.{data_type}{staging_table_suffix}",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            max_bad_records=0,
            time_partitioning=time_partition,
            cluster_fields=cluster_fields,
            export_task_id=export_task_id,
            failed_transforms="{{ task_instance.xcom_pull(task_ids='"
            + export_task_id
            + '\')["failed_transforms"] }}',
            max_failed_transforms=0,
            on_failure_callback=alert_after_max_retries,
            dag=dag,
        )

    else:
        schema_fields = read_local_schema(f"{data_type}")
        return GCSToBigQueryOperator(
            task_id=f"send_{data_type}_to_{dataset_type}",
            execution_timeout=timedelta(
                seconds=Variable.get("task_timeout", deserialize_json=True)[
                    build_gcs_to_bq_task.__name__
                ]
            ),
            bucket=bucket_name,
            schema_fields=schema_fields,
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=[
                "{{ task_instance.xcom_pull(task_ids='"
                + export_task_id
                + '\')["output"] }}'
                + source_object_suffix
            ],
            destination_project_dataset_table=f"{project_name}.{dataset_name}.{data_type}{staging_table_suffix}",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            max_bad_records=0,
            time_partitioning=time_partition,
            cluster_fields=cluster_fields,
            on_failure_callback=alert_after_max_retries,
            dag=dag,
        )
