"""
The mgi_pipeline_dag DAG updates the Mgi table in Bigquey every day.
"""

import datetime
import json
import logging
from tabnanny import check

from airflow import DAG
from airflow.models import Variable

from stellar_etl_airflow.build_gcs_csv_to_bq_task import build_gcs_csv_to_bq_task , build_check_gcs_file
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "mgi_pipeline_dag",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 1, 11, 0, 0),
    description="This DAG updates the Mgi table in Bigquey every day",
    schedule_interval="20 15 * * *",
    params={
        "alias": "mgi",
    },
    render_template_as_native_obj=True,
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    catchup=False,
)

internal_project = Variable.get("bq_project")
internal_dataset = Variable.get("bq_dataset")
bucket_name = Variable.get("bucket_mgi")


check_gcs_file = build_check_gcs_file('check_gcs_file', dag, bucket_name)


send_mgi_to_bq_internal_task = build_gcs_csv_to_bq_task('send_mgi_to_bq_pub_task', dag, internal_project, internal_dataset, 'raw_mgi_stellar_transactions', bucket_name)


check_gcs_file >> send_mgi_to_bq_internal_task
