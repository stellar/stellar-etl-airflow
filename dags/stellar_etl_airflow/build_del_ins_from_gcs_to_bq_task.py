from datetime import timedelta

from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from sentry_sdk import capture_message, push_scope
from stellar_etl_airflow import macros
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

    def pre_execute(self, context, **kwargs):
        # Expand the template fields using Jinja2 templating from the context
        self.failed_transforms = self.render_template(self.failed_transforms, context)
        self.max_failed_transforms = self.render_template(
            self.max_failed_transforms, context
        )
        self.export_task_id = self.render_template(self.export_task_id, context)

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
        super().pre_execute(context, **kwargs)


def build_del_ins_from_gcs_to_bq_task(
    project,
    dataset,
    table_id,
    table_name,
    export_task_id,
    source_object_suffix,
    partition,
    cluster,
    batch_id,
    batch_date,
    source_objects,
    **context,
):
    dag = context["dag"]
    staging_table_suffix = ""
    if table_name == "history_assets":
        staging_table_suffix = "_staging"

    # Delete operation

    DELETE_ROWS_QUERY = (
        f"DELETE FROM {dataset}.{table_name}{staging_table_suffix} "
        f"WHERE batch_run_date = '{batch_date}'"
        f"AND batch_id = '{batch_id}';"
    )
    delete_task = BigQueryInsertJobOperator(
        project_id=project,
        task_id=f"delete_old_partition_{table_name}_bq",
        execution_timeout=timedelta(
            seconds=Variable.get("task_timeout", deserialize_json=True)[
                build_del_ins_from_gcs_to_bq_task.__name__
            ]
        ),
        sla=timedelta(
            seconds=Variable.get("task_sla", deserialize_json=True)[
                build_del_ins_from_gcs_to_bq_task.__name__
            ]
        ),
        configuration={
            "query": {
                "query": DELETE_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
    )
    delete_task.execute(context)

    # Insert operation

    bucket_name = Variable.get("gcs_exported_data_bucket_name")
    history_tables = [
        "ledgers",
        "assets",
        "transactions",
        "operations",
        "trades",
        "effects",
        "contract_events",
    ]

    if cluster:
        cluster_fields = Variable.get("cluster_fields", deserialize_json=True)
        cluster_fields = (
            cluster_fields[f"{table_name}"]
            if table_id in history_tables
            else cluster_fields[table_name]
        )
    else:
        cluster_fields = None

    time_partition = {}
    if partition:
        partition_fields = Variable.get("partition_fields", deserialize_json=True)
        partition_fields = (
            partition_fields[f"{table_name}"]
            if table_id in history_tables
            else partition_fields[table_name]
        )
        time_partition["type"] = partition_fields["type"]
        time_partition["field"] = partition_fields["field"]

    schema_fields = read_local_schema(f"{table_name}")

    if table_id in history_tables:
        gcs_to_bq_operator = CustomGCSToBigQueryOperator(
            task_id=f"send_{table_name}_to_bq",
            execution_timeout=timedelta(
                seconds=Variable.get("task_timeout", deserialize_json=True)[
                    build_del_ins_from_gcs_to_bq_task.__name__
                ]
            ),
            bucket=bucket_name,
            schema_fields=schema_fields,
            schema_update_options=["ALLOW_FIELD_ADDITION"],
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=source_objects,
            destination_project_dataset_table=f"{project}.{dataset}.{table_name}{staging_table_suffix}",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            # schema_update_option="ALLOW_FIELD_ADDITION",
            max_bad_records=0,
            time_partitioning=time_partition,
            cluster_fields=cluster_fields,
            export_task_id=export_task_id,
            # TODO: failed_transforms can be set to 0 because the task will fail upstream if there are any failed transforms.
            # Refactor this logic later to clean up unused code.
            failed_transforms="0",
            max_failed_transforms=0,
            sla=timedelta(
                seconds=Variable.get("task_sla", deserialize_json=True)[
                    build_del_ins_from_gcs_to_bq_task.__name__
                ]
            ),
            dag=dag,
        )
    else:
        gcs_to_bq_operator = GCSToBigQueryOperator(
            task_id=f"send_{table_name}_to_bq",
            execution_timeout=timedelta(
                seconds=Variable.get("task_timeout", deserialize_json=True)[
                    build_del_ins_from_gcs_to_bq_task.__name__
                ]
            ),
            bucket=bucket_name,
            schema_fields=schema_fields,
            schema_update_options=["ALLOW_FIELD_ADDITION"],
            autodetect=False,
            source_format="NEWLINE_DELIMITED_JSON",
            source_objects=source_objects,
            destination_project_dataset_table=f"{project}.{dataset}.{table_name}{staging_table_suffix}",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
            max_bad_records=0,
            time_partitioning=time_partition,
            cluster_fields=cluster_fields,
            sla=timedelta(
                seconds=Variable.get("task_sla", deserialize_json=True)[
                    build_del_ins_from_gcs_to_bq_task.__name__
                ]
            ),
            dag=dag,
        )

    gcs_to_bq_operator.execute(context)
