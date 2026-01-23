from datetime import timedelta

from airflow.sensors.external_task import ExternalTaskSensor
from stellar_etl_airflow.default import alert_after_max_retries


def build_cross_deps(dag, task, parent_dag, parent_task=None, time_delta=0, timeout=3600):
    return ExternalTaskSensor(
        task_id=f"check_{task}_finish",
        external_dag_id=parent_dag,
        external_task_id=parent_task,  # None means wait for the entire DAG to finish
        execution_delta=timedelta(minutes=time_delta),
        on_failure_callback=alert_after_max_retries,
        timeout=timeout,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
    )
