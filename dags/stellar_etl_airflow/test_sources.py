import re
from datetime import datetime, timedelta

import pendulum
import pytz
from airflow import DAG, settings
from airflow.models import DagBag, DagRun, TaskInstance, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.state import State
from google.cloud import bigquery, storage
from google.oauth2 import service_account


def treating_errors(successful_transforms, BQ_results):
    if successful_transforms["ledgers"] != BQ_results["ledgers"]:
        print(
            "bq_ledgers are {0} and successful_transforms ledgers are {1}".format(
                BQ_results["ledgers"], successful_transforms["ledgers"]
            )
        )
        raise ValueError("Mismatch between operations in GCS and BQ operations")

    elif successful_transforms["operations"] != BQ_results["operations"]:
        print(
            "bq_operations are {0} and successful_transforms operations are {1}".format(
                BQ_results["operations"], successful_transforms["operations"]
            )
        )
        raise ValueError("Mismatch between operations in GCS and BQ operations")

    elif successful_transforms["trades"] != BQ_results["trades"]:
        print(
            "bq trades are {0} and successful_transforms trades are {1}".format(
                BQ_results["trades"], successful_transforms["trades"]
            )
        )
        raise ValueError("Mismatch between trades in GCS and BQ trades")

    elif successful_transforms["effects"] != BQ_results["effects"]:
        print(
            "bq effects are {0} and successful_transforms effects are {1}".format(
                BQ_results["effects"], successful_transforms["effects"]
            )
        )
        raise ValueError("Mismatch between effects in GCS and BQ effects")

    elif successful_transforms["transactions"] != BQ_results["transactions"]:
        print(
            "bq transactions are {0} and successful_transforms transactions are {1}".format(
                BQ_results["transactions"], successful_transforms["transactions"]
            )
        )
        raise ValueError("Mismatch between transactions in GCS and BQ transactions")


def do_query(opType, date):
    key_path = Variable.get("api_key_path")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    query_job = client.query(
        f"""SELECT
        (SELECT COUNT(*) FROM crypto-stellar.crypto_stellar.history_{opType}
        WHERE DATE(batch_run_date)='{date.strftime("%Y-%m-%d")}')
        """
    )
    return query_job


def store_files(file_names, successful_transforms_folders):
    for key in successful_transforms_folders.keys():
        # Use a list comprehension with a regex to get the matching file names
        matching_files = [file for file in file_names if re.search(rf"{key}", file)]
        successful_transforms_folders[key] = matching_files
    return successful_transforms_folders


def get_from_stateTables(**context):
    successful_transforms = {
        "account_signers": 0,
        "accounts": 0,
        "claimable_balances": 0,
        "liquidity_pools": 0,
        "offers": 0,
        "trust_lines": 0,
        "offers": 0,
        "config_settings": 0,
        "contract_code": 0,
        "contract_data": 0,
        "ttl": 0,
    }
    successful_transforms_folders = {
        "account_signers": None,
        "accounts": None,
        "claimable_balances": None,
        "liquidity_pools": None,
        "offers": None,
        "trust_lines": None,
        "offers": None,
        "config_settings": None,
        "contract_code": None,
        "contract_data": None,
        "ttl": None,
    }

    execution_date = context["execution_date"]
    yesterday = pendulum.instance(execution_date).subtract(days=1)

    # yesterday = datetime(2024, 4, 16, 0, 0, tzinfo=pytz.UTC)

    session = settings.Session()

    # Get all the execution dates for the current date (yesterday)
    execution_dates = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "state_table_export",
            DagRun.execution_date >= yesterday.start_of("day"),
            DagRun.execution_date < yesterday.add(days=1).start_of("day"),
            DagRun.state == State.SUCCESS,
        )
        .all()
    )

    gcs_hook = GCSHook(google_cloud_storage_conn_id="google_cloud_storage_default")

    key_path = Variable.get("api_key_path")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    storage_client = storage.Client(credentials=credentials)

    for dag_run in execution_dates:
        execution_date_str = dag_run.execution_date.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        ).replace(" ", "T")
        execution_date_str = execution_date_str[:-2] + ":" + execution_date_str[-2:]

        bucket = storage_client.get_bucket(f"us-central1-test-hubble-2-5f1f2dbf-bucket")
        blobs = bucket.list_blobs(
            prefix=f"/dag-exported/scheduled__{execution_date_str}/changes_folder"
        )

        for blob in blobs:
            print(blob.name)

        # regex to find the name of each table in file names, example files belonging to "...offers.txt"
        # successful_transforms_folders = store_files(
        #     files, successful_transforms_folders
        # )

        ## Download the file and get its content, it runs 47 times day 16th of april
        # file_content = gcs_hook.download(
        #    bucket_name="us-central1-test-hubble-2-5f1f2dbf-bucket",
        #    object_name=f"dag-exported/us-central1-hubble-2-d948d67b-bucket/dag-exported/scheduled__{execution_date_str}/changes_folder",
        # )

        ## Decode the bytes object to a string
        # file_content = file_content.decode()

        ## Now file_content is a string with the content of the file
        # lines = file_content.splitlines()

        ## Count the number of lines that start with "{"
        # count = sum(1 for line in lines if line.startswith("{"))

        # print(count)


def get_from_historyTableExport(**context):
    successful_transforms = {
        "ledgers": 0,
        "operations": 0,
        "trades": 0,
        "effects": 0,
        "transactions": 0,
    }

    # execution_date = context["execution_date"]
    # yesterday = pendulum.instance(execution_date)  # .subtract(days=1)
    # yesterday = datetime.combine(yesterday, time(), tzinfo=pytz.timezone("UTC"))
    yesterday = pendulum.datetime(2024, 4, 23, 16, 30).in_timezone("UTC")

    # Get the session from the settings
    session = settings.Session()

    # Get all the execution dates for the current date (yesterday)
    execution_dates = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "history_table_export",
            DagRun.execution_date == yesterday,
            DagRun.state == State.SUCCESS,
        )
        .all()
    )

    dag_bag = DagBag()
    dag = dag_bag.get_dag("history_table_export")

    for key in successful_transforms.keys():
        task = dag.get_task("export_" + key + "_task")

        total_successful_transforms = 0

        for dag_run in execution_dates:
            # Retrieve successful_transforms from XCOM
            ti = TaskInstance(task, dag_run.execution_date)
            xcom_return = ti.xcom_pull(task_ids=task.task_id, key="return_value")

            # Parse JSON and get successful_transforms
            successful_transforms_op = xcom_return["successful_transforms"]
            total_successful_transforms += successful_transforms_op

        successful_transforms[key] += total_successful_transforms

    # Query number of rows in BigQuery table
    query_job = do_query("ledgers", yesterday)
    query_job1 = do_query("operations", yesterday)
    query_job2 = do_query("trades", yesterday)
    query_job3 = do_query("effects", yesterday)
    query_job4 = do_query("transactions", yesterday)

    BQ_results = {
        "ledgers": next(iter(query_job.result()))[0],
        "operations": next(iter(query_job1.result()))[0],
        "trades": next(iter(query_job2.result()))[0],
        "effects": next(iter(query_job3.result()))[0],
        "transactions": next(iter(query_job4.result()))[0],
    }

    context["ti"].xcom_push(key="from BQ", value=BQ_results)
    context["ti"].xcom_push(key="from GCS", value=successful_transforms)

    treating_errors(successful_transforms, BQ_results)


dag = DAG(
    "daily_test_sources",
    start_date=datetime(2024, 4, 15, 0, 0),
    schedule_interval=timedelta(days=1),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)

compare_task = PythonOperator(
    task_id="get_from_historyTableExport_task",
    python_callable=get_from_historyTableExport,
    provide_context=True,
    dag=dag,
)

compare2_task = PythonOperator(
    task_id="get_from_stateTables_task",
    python_callable=get_from_stateTables,
    provide_context=True,
    dag=dag,
)
