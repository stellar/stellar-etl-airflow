import logging
from datetime import datetime, timedelta

from airflow.models import Variable
from sentry_sdk import capture_message, init, push_scope, set_tag
from sentry_sdk.integrations.logging import LoggingIntegration


def init_sentry():
    sentry_logging = LoggingIntegration(level=logging.INFO, event_level=logging.FATAL)

    init(
        dsn=Variable.get("sentry_dsn"),
        environment=Variable.get("sentry_environment"),
        integrations=[
            sentry_logging,
        ],
    )
    set_tag("image_version", Variable.get("image_name"))


def get_base_dag_args():
    return {
        "owner": Variable.get("owner"),
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }


def get_default_dag_args():
    base = get_base_dag_args()
    base["start_date"] = datetime(2015, 9, 30)
    return base


def get_orderbook_dag_args():
    base = get_base_dag_args()
    base["start_date"] = datetime(2015, 9, 30)
    return base


def alert_after_max_retries(context):
    """
    When a task is cleared, the try_numbers continue to increment.
    This returns the try number relative to the last clearing.
    """
    ti = context["task_instance"]

    with push_scope() as scope:
        scope.set_tag("data-quality", "max-retries")
        scope.set_extra("dag_id", ti.dag_id)
        scope.set_extra("task_id", ti.task_id)
        scope.set_extra("run_id", ti.run_id)
        scope.set_extra("duration", ti.duration)
        capture_message(
            f"The task {ti.task_id} belonging to DAG {ti.dag_id} failed after max retries.",
            "fatal",
        )


def alert_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    When a task takes longer then expected to run while having a defined SLA,
    it misses it.
    This alerts the IT team about the unexpected behavior in order
    to enable faster response in case of underlying infrastructure issues.
    """
    dag_id = slas[0].dag_id
    task_id = slas[0].task_id
    execution_date = slas[0].execution_date.isoformat()

    with push_scope() as scope:
        scope.set_extra("dag_id", dag_id)
        scope.set_extra("task_id", task_id)
        scope.set_extra("execution_date", execution_date)
        capture_message(
            f"SLA Miss! The task {task_id} belonging to DAG {dag_id} missed its SLA for the run date {execution_date}.",
            "warn",
        )
