import logging
import time

from airflow.configuration import conf
from airflow.utils.state import TaskInstanceState

base_log_folder = conf.get("logging", "base_log_folder")


def get_log_file_name(context):
    ti = context["ti"]
    log_params = [
        base_log_folder,
        f"dag_id={ti.dag_id}",
        f"run_id={ti.run_id}",
        f"task_id={ti.task_id}",
        f"attempt={ti.try_number - 1}.log",
    ]
    return "/".join(log_params)


def check_dbt_transient_errors(context):
    """
    Searches through the logs to find failure messages
    and returns True if the errors found are transient.
    """
    dbt_transient_error_message = "Could not serialize access to table"

    log_file_path = get_log_file_name(context)
    log_contents = read_log_file(log_file_path)

    for line in log_contents:
        if dbt_transient_error_message in line:
            return True
    return False


def read_log_file(log_file_path):
    max_retries = 3
    retry_delay = 30
    log_contents = []

    for attempt in range(max_retries):
        try:
            with open(log_file_path, "r") as log_file:
                log_contents = log_file.readlines()
            break
        except FileNotFoundError as file_error:
            if attempt < max_retries - 1:
                logging.warn(
                    f"Log file {log_file_path} not found retrying in {retry_delay} seconds..."
                )
                time.sleep(retry_delay)
            else:
                logging.error(file_error)
                raise
    return log_contents


def skip_retry_callback(context) -> None:
    """
    Set task state to SKIPPED in case errors found in dbt are not transient.
    """
    if not check_dbt_transient_errors(context):
        ti = context["ti"]
        logging.info(
            f"Set task instance {ti} state to \
                {TaskInstanceState.SKIPPED} to skip retrying"
        )
        ti.set_state(TaskInstanceState.SKIPPED)
        return
    else:
        return
