import base64
import logging
import re
import time

from airflow.configuration import conf
from airflow.models import Variable
from airflow.utils.state import TaskInstanceState
from google.cloud import secretmanager

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
    log_file_path = get_log_file_name(context)
    log_contents = read_log_file(log_file_path)

    dbt_transient_error_patterns = Variable.get(
        "dbt_transient_errors_patterns", deserialize_json=True
    )

    dbt_summary_line = None
    for line in log_contents:
        # Check for transient errors message patterns
        for transient_error, patterns in dbt_transient_error_patterns.items():
            if all(sentence in line for sentence in patterns):
                logging.info(
                    f"Found {transient_error} dbt transient error, proceeding to retry"
                )
                return True
            elif "Done. PASS=" in line:
                dbt_summary_line = line
                break
    # Check if dbt summary has been logged
    if dbt_summary_line:
        match = re.search(r"ERROR=(\d+)", dbt_summary_line)
        if match:
            dbt_errors = int(match.group(1))
            # Check if dbt pipeline returned errors
            if dbt_errors > 0:
                logging.info("Could not find dbt transient errors, skipping retry")
                return False
            else:
                logging.info(
                    "dbt pipeline finished without errors, task failed but will not retry"
                )
                return False
    # Logic could not identify the error and assumes it is transient
    logging.info("Task failed due to unforeseen error, proceeding to retry")
    return True


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


def skip_retry_dbt_errors(context) -> None:
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


def access_secret(secret_name):
    """
    Access a secret from Google Secret Manager.
    
    Args:
        secret_name (str): Name of the secret in GSM (without prefixes)
        project_id (str): GCP project ID
        
    Returns:
        str: secret value
    """
    client = secretmanager.SecretManagerServiceClient()
    project_id = Variable.get("bq_project")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    secret_value = response.payload.data.decode("UTF-8")
    return secret_value
