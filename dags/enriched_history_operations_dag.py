"""
The `enriched_history_operations` DAG runs `dbt build` the `enriched_history_operations` model.
"""
from datetime import time

from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args

dag = DAG(
    "enriched_history_operations",
    default_args=get_default_dag_args(),
    schedule_interval=None,  # don’t schedule since this DAG is externally triggered daily
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=False,
    tags=["dbt"],
)

"""
This task is created by the function dbt_task() that has `build`
as the default argument for `dbt` and the default graph operator is `+`.
The final command is similar to: `dbt build --select +enriched_history_operations`
"""
enriched_history_operations_task = dbt_task(
    dag, model_name="enriched_history_operations"
)

"""
The `last_dag_run_task` will only return `True` when it's 00:00 UTC
then it will follow the task id `trigger_dbt_daily_dag`.
This logic along with tasks dependencies will result in the `dbt_daily_dag`
running once a daily (midnight).
"""
last_dag_run_task = BranchDateTimeOperator(
    task_id="last_dag_run_task",
    use_task_logical_date=True,
    follow_task_ids_if_true=["trigger_dbt_daily_dag"],
    follow_task_ids_if_false=["not_last_dag_run_task"],
    target_upper=time(0, 0, 1),
    target_lower=time(0, 0, 0),
    dag=dag,
)

"""
This task triggers the `trigger_dbt_daily_dag` that's resposible for running the dbt `dbt_daily` model.
The execution date of the triggered DAG will be the same as this DAG.
The `reset_dag_run` boolean set to `True` means that whenever this DAG is rerun/cleared, the triggered DAG also receives a new DAG run.
The `wait_for_completion` boolean set to `True` means that this task will wait until the triggered DAG finishes.
"""
trigger_dbt_daily_dag = TriggerDagRunOperator(
    task_id="trigger_dbt_daily_dag",
    trigger_dag_id="dbt_daily",
    execution_date="{{ ts }}",
    reset_dag_run=True,
    wait_for_completion=True,
    dag=dag,
)

"""
The `midday_run_task` will only return `True` when it's 12:00 UTC
then it will follow the task id `trigger_dbt_ohlc_dag`.
This logic along with tasks dependencies will result in the `dbt_ohlc_dag`
running twice a day (noon and midnight).
"""
midday_run_task = BranchDateTimeOperator(
    task_id="midday_run_task",
    use_task_logical_date=True,
    follow_task_ids_if_true=["trigger_dbt_ohlc_dag"],
    follow_task_ids_if_false=["not_midday_dag_run_task"],
    target_upper=time(12, 0, 1),
    target_lower=time(12, 0, 0),
    dag=dag,
)

"""
This task triggers the `trigger_dbt_ohlc_dag` that's resposible for running the dbt `dbt_ohlc` model.
The execution date of the triggered DAG will be the same as this DAG.
The `reset_dag_run` boolean set to `True` means that whenever this DAG is rerun/cleared, the triggered DAG also receives a new DAG run.
The `wait_for_completion` boolean set to `False` means that this task won't wait until the triggered DAG finishes.
"""
trigger_dbt_ohlc_dag = TriggerDagRunOperator(
    task_id="trigger_dbt_ohlc_dag",
    trigger_dag_id="dbt_ohlc",
    trigger_rule="none_failed_min_one_success",
    execution_date="{{ ts }}",
    reset_dag_run=True,
    wait_for_completion=False,
    dag=dag,
)

"""
Placeholder tasks to organize the flow of BranchDateTimeOperator
"""
not_last_dag_run_task = EmptyOperator(task_id="not_last_dag_run_task")
not_midday_dag_run_task = EmptyOperator(task_id="not_midday_dag_run_task")

enriched_history_operations_task >> [last_dag_run_task, midday_run_task]
last_dag_run_task >> [
    not_last_dag_run_task,
    trigger_dbt_daily_dag,
]
trigger_dbt_daily_dag >> trigger_dbt_ohlc_dag
midday_run_task >> [not_midday_dag_run_task, trigger_dbt_ohlc_dag]
