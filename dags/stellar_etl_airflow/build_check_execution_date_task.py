from datetime import datetime

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from stellar_etl_airflow.default import alert_after_max_retries


def check_execution_date(**kwargs):
    date_for_resets = Variable.get("date_for_resets", deserialize_json=True)
    date_for_resets = date_for_resets["date"]
    today = datetime.today().date()
    for date in date_for_resets:
        date = datetime.strptime(date, "%Y-%m-%d").date()
        if today == date:
            return kwargs.get("start_task")
    return kwargs.get("stop_task")


def build_check_execution_date(dag, task_name, start_task, stop_task):
    return BranchPythonOperator(
        task_id=task_name,
        python_callable=check_execution_date,
        op_kwargs={"start_task": start_task, "stop_task": stop_task},
        provide_context=True,
        dag=dag,
    )


def path_to_execute(dag, task_name):
    return DummyOperator(task_id=task_name, dag=dag)
