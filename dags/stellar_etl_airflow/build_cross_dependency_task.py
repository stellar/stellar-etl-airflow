from datetime import timedelta

from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from airflow.utils.state import State
from stellar_etl_airflow.default import alert_after_max_retries


class LatestDagRunSensor(BaseSensorOperator):
    """Waits for the latest successful DagRun whose execution_date meets a minimum."""

    def __init__(
        self,
        external_dag_id,
        min_execution_date_fn=None,
        poke_interval=60,
        timeout=3600,
        **kwargs,
    ):
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            mode="reschedule",
            **kwargs,
        )
        self.external_dag_id = external_dag_id
        self.min_execution_date_fn = min_execution_date_fn or (lambda context: None)

    @provide_session
    def poke(self, context, session=None):
        min_date = self.min_execution_date_fn(context)
        query = session.query(DagRun).filter(
            DagRun.dag_id == self.external_dag_id, DagRun.state == State.SUCCESS
        )
        if min_date:
            query = query.filter(DagRun.execution_date >= min_date)
        run = query.order_by(DagRun.execution_date.desc()).first()
        if run:
            self.log.info(
                "Found successful run %s (%s) for %s",
                run.run_id,
                run.execution_date.isoformat(),
                self.external_dag_id,
            )
            return True
        self.log.info(
            "Waiting for %s to succeed on or after %s",
            self.external_dag_id,
            min_date,
        )
        return False


def build_cross_deps(
    dag,
    task,
    parent_dag,
    parent_task=None,
    time_delta=0,
    timeout=3600,
    execution_date_fn=None,
):
    execution_delta = None if execution_date_fn else timedelta(minutes=time_delta)

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
