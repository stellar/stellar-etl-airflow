from datetime import datetime, timedelta

from airflow.models import Variable
from sentry_sdk import capture_message, init, push_scope, set_tag


def init_sentry():
    init(
        dsn=Variable.get("sentry_dsn"),
        environment=Variable.get("sentry_environment"),
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
    base["start_date"] = datetime(2021, 8, 31)
    return base


def get_orderbook_dag_args():
    base = get_base_dag_args()
    base["start_date"] = datetime(2021, 8, 31)
    return base


def alert_after_max_retries(context):
    """
    When a task is cleared, the try_numbers continue to increment.
    This returns the try number relative to the last clearing.
    """
    ti = context["task_instance"]
    actual_try_number = ti.try_number
    relative_first_try = ti.max_tries - ti.task.retries + 1
    relative_try = actual_try_number - relative_first_try + 1

    if int(relative_try) >= int(ti.max_tries):
        with push_scope() as scope:
            scope.set_tag("data-quality", "max-retries")
            scope.set_extra("dag_id", ti.dag_id)
            scope.set_extra("task_id", ti.task_id)
            scope.set_extra("run_id", ti.run_id)
            scope.set_extra("duration", ti.duration)
            capture_message(
                f"The task {ti.task_id} belonging to DAG {ti.dag_id} failed after max retries.",
                "error",
            )
