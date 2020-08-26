import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_date_task import build_date_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG

dag = DAG(
    'bounded_core_export',
    default_args=get_default_dag_args(),
    description='This DAG runs a bounded stellar-core instance, which allows it to export accounts, offers, and trustlines to BigQuery.',
    schedule_interval="*/5 * * * *",
)

date_task = build_date_task(dag)

changes_task = build_export_task(dag, 'bounded-core', 'export_ledger_entry_changes', 'changes.txt')

date_task >> changes_task