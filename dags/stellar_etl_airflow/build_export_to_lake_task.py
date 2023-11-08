"""
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into Google Cloud Storage.
"""

import json
import sys

from airflow.models import Variable
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from stellar_etl_airflow.default import alert_after_max_retries


def get_ledger_sequence(ledger_range_task_id):
    return "{{ task_instance.xcom_pull(task_ids='" + ledger_range_task_id + "') }}"


def transform_ledger_sequence(ledger_range):
    src_file = sys.argv[1]
    dest_file = sys.argv[2]
    target_ledger = []
    with open(src_file) as f:
        raw_data = f.readlines()
        for data in raw_data:
            data = json.loads(data)
            if data["ledger_sequence"] == ledger_range:
                target_ledger.append(data)
    with open(dest_file, "w") as f:
        f.write(str(target_ledger))
        return dest_file


def export_to_lake(dag, export_task_id, ledger_sequence):
    bucket_source = Variable.get("gcs_exported_data_bucket_name")
    bucket_destination = Variable.get("ledger_transaction_data_lake_bucket_name")
    destination_data = [
        "{{ task_instance.xcom_pull(task_ids='"
        + export_task_id
        + '\')["output"][13:] }}'
    ]

    return GCSFileTransformOperator(
        task_id="export_data_to_lake",
        source_bucket=bucket_source,
        source_object=[
            "{{ task_instance.xcom_pull(task_ids='"
            + export_task_id
            + '\')["output"] }}'
        ],
        destination_bucket=bucket_destination,
        destination_object=f"{ledger_sequence}.txt",
        transform_script=transform_ledger_sequence,
        op_kwargs={
            "ledger_sequence": ledger_sequence,
        },
        on_failure_callback=alert_after_max_retries,
    )
