from datetime import timedelta

from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from airflow.utils.state import State
from stellar_etl_airflow.default import alert_after_max_retries


def latest_successful_execution_date_fn(
    external_dag_id, min_execution_date_fn=None
):
    @provide_session
    def _fn(context, session=None):
        query = (
            session.query(DagRun)
            .filter(DagRun.dag_id == external_dag_id, DagRun.state == State.SUCCESS)
        )

        if min_execution_date_fn:
            min_date = min_execution_date_fn(context)
            if min_date:
                query = query.filter(DagRun.execution_date >= min_date)

        run = query.order_by(DagRun.execution_date.desc()).first()
        return run.execution_date if run else None

    return _fn


def build_cross_deps(
    dag,
    task,
    parent_dag,
    parent_task=None,
    time_delta=0,
    timeout=3600,
    execution_date_fn=None,
):
    execution_delta = (
        None if execution_date_fn else timedelta(minutes=time_delta)
    )

    return ExternalTaskSensor(
        task_id=f"check_{task}_finish",
        external_dag_id=parent_dag,
        external_task_id=parent_task,  # None means wait for the entire DAG to finish
        execution_delta=execution_delta,
        execution_date_fn=execution_date_fn,
        on_failure_callback=alert_after_max_retries,
        timeout=timeout,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
    )
