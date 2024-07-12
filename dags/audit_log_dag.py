"""
This DAG runs an audit log SQL to update the audit log dashboard.
"""

from datetime import datetime
from json import loads

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from stellar_etl_airflow.build_bq_insert_job_task import build_bq_insert_job
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "audit_log_dag",
    default_args=get_default_dag_args(),
    description="This DAG runs periodically to update the audit log dashboard.",
    start_date=datetime(2023, 1, 1, 0, 0),
    schedule_interval="10 9 * * *",
    params={
        "alias": "audit-log",
    },
    user_defined_filters={"fromjson": lambda s: loads(s)},
    catchup=False,
)


table_name = "audit_log"
project = Variable.get("bq_project")
dataset = Variable.get("bq_dataset_audit_log")


build_audit_log_bq_job_task = build_bq_insert_job(
    dag,
    project,
    dataset,
    table_name,
    partition=False,
    cluster=False,
    create=True,
    write_disposition="WRITE_TRUNCATE",
)

start_dag = DummyOperator(task_id="start_dag", dag=dag)

start_dag >> build_audit_log_bq_job_task
