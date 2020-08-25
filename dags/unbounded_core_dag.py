import json

from datetime import timedelta
from subprocess import Popen
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_date_task import build_date_task

from airflow import DAG, AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


default_args = {
    'owner': 'isaiahturner',
    'depends_on_past': False,
    'start_date': "2015-09-30T16:46:54+00:00",
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'unbounded_core_export',
    default_args=default_args,
    description='This DAG runs an unbounded stellar-core instance, which allows it to export accounts, offers, and trustlines to BigQuery. The core instance will \
        continue running and exporting in the background.',
    schedule_interval="*/5 * * * *",
)

date_task = build_date_task(dag)

changes_task = build_export_task(dag, 'unbounded-core', 'export_ledger_entry_changes', 'changes.txt')

date_task >> changes_task