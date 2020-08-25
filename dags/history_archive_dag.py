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
    'history_archive_export',
    default_args=default_args,
    description='This DAG exports ledgers, transactions, and operations from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
)

date_task = build_date_task(dag)

ledger_task = build_export_task(dag, 'archive', 'export_ledgers', 'ledgers.txt')

tx_task = build_export_task(dag, 'archive', 'export_transactions', 'transactions.txt')

op_task = build_export_task(dag, 'archive', 'export_operations', 'operations.txt')

date_task >> ledger_task
date_task >> tx_task
date_task >> op_task