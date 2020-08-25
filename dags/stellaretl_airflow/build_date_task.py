import json

from datetime import timedelta
from subprocess import Popen

from airflow import DAG, AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

def build_date_task(dag):
    return BashOperator(
        task_id='get_ledger_range_from_times',
        bash_command='stellar-etl get_ledger_range_from_times -s {{ ts }} -e {{ next_execution_date.isoformat() }} --stdout',
        dag=dag,
        xcom_push=True,
    )