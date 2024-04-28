import os
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


def comparison(**context):
    execution_date = context["execution_date"]
    yesterday = pendulum.instance(execution_date).subtract(days=3)

    # Define the table names
    tables1 = ["ledgers", "operations", "trades", "effects", "transactions"]
    tables2 = [
        "account_signers",
        "accounts",
        "claimable_balances",
        "liquidity_pools",
        "offers",
        "trust_lines",
        "config_settings",
        "contract_code",
        "contract_data",
        "ttl",
    ]

    BQ_results = {}
    internal_results = {}

    # Iterate over the tables and call do_query for each table
    for table in tables1:
        query_job = do_query("crypto-stellar.crypto_stellar", table, yesterday, True)
        BQ_results[table] = next(iter(query_job.result()))[0]

        # query_job_2 = do_query(
        #    "hubble-261722.crypto_stellar_internal_2", table, yesterday, True
        # )
        # internal_results[table] = next(iter(query_job_2.result()))[0]

    # Iterate over the tables and call do_query2 for each table
    for table in tables2:
        query_job = do_query("crypto-stellar.crypto_stellar", table, yesterday)
        BQ_results[table] = next(iter(query_job.result()))[0]

        # query_job_2 = do_query(
        #    "hubble-261722.crypto_stellar_internal_2", table, yesterday
        # )
        # internal_results[table] = next(iter(query_job_2.result()))[0]

    context["ti"].xcom_push(key="from BQ", value=BQ_results)
    # context["ti"].xcom_push(key="from internal", value=internal_results)


def do_query(project_dataset, opType, date, history=False):
    key_path = Variable.get("api_key_path")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(
        credentials=credentials, project=credentials.project_id, location="US"
    )

    print(f"OpType is {opType} and date is {date.strftime('%Y-%m-%d')}")

    # Add 'history_' to the table name if history is True
    table_name = f"history_{opType}" if history else opType

    query_job = client.query(
        f"""SELECT
        (SELECT COUNT(*) FROM {project_dataset}.{table_name}
        WHERE batch_run_date>='{date.strftime("%Y-%m-%d")}' and batch_run_date<'{date.add(days=1).strftime("%Y-%m-%d")}')
        """
    )
    return query_job


def get_from_stateTables(**context):
    successful_transforms = {
        "signers": 0,
        "accounts": 0,
        "claimable_balances": 0,
        "liquidity_pools": 0,
        "offers": 0,
        "trustlines": 0,
        "offers": 0,
        "config_settings": 0,
        "contract_code": 0,
        "contract_data": 0,
        "ttl": 0,
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

    key_path = Variable.get("api_key_path")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    storage_client = storage.Client(credentials=credentials)
    gcs_hook = GCSHook(google_cloud_storage_conn_id="google_cloud_storage_default")

    for dag_run in execution_dates:
        execution_date_str = dag_run.execution_date.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        ).replace(" ", "T")
        execution_date_str = execution_date_str[:-2] + ":" + execution_date_str[-2:]

        bucket = storage_client.get_bucket("us-central1-test-hubble-2-5f1f2dbf-bucket")
        blobs = list(
            bucket.list_blobs(
                prefix=f"dag-exported/scheduled__{execution_date_str}/changes_folder"
            )
        )

        for key in successful_transforms.keys():
            count = 0

            for blob in blobs:
                if re.search(rf"{key}", blob.name):
                    file_content = gcs_hook.download(
                        bucket_name="us-central1-test-hubble-2-5f1f2dbf-bucket",
                        object_name=f"dag-exported/scheduled__{execution_date_str}/changes_folder/{os.path.basename(blob.name)}",
                    )

                    # Decode the bytes object to a string
                    file_content = file_content.decode()

                    ## Now file_content is a string with the content of the file
                    lines = file_content.splitlines()

                    print(f"O FILE CONTENT SPLITADO E: {lines} ")

                    ## Count the number of lines that start with "{"
                    count = sum(1 for line in lines if line.startswith("{"))

                    successful_transforms[key] += count

    for key in successful_transforms.keys():
        print(f"Successful transforms for {key} is {successful_transforms[key]}")

    # Query number of rows in BigQuery table
    query_job = do_query("crypto-stellar.crypto_stellar", "account_signers", yesterday)
    query_job1 = do_query("crypto-stellar.crypto_stellar", "accounts", yesterday)
    query_job2 = do_query(
        "crypto-stellar.crypto_stellar", "claimable_balances", yesterday
    )
    query_job3 = do_query("crypto-stellar.crypto_stellar", "liquidity_pools", yesterday)
    query_job4 = do_query("crypto-stellar.crypto_stellar", "offers", yesterday)
    query_job5 = do_query("crypto-stellar.crypto_stellar", "trust_lines", yesterday)
    query_job6 = do_query("crypto-stellar.crypto_stellar", "config_settings", yesterday)
    query_job7 = do_query("crypto-stellar.crypto_stellar", "contract_code", yesterday)
    query_job8 = do_query("crypto-stellar.crypto_stellar", "contract_data", yesterday)
    query_job9 = do_query("crypto-stellar.crypto_stellar", "ttl", yesterday)

    BQ_results = {
        "signers": next(iter(query_job.result()))[0],
        "accounts": next(iter(query_job1.result()))[0],
        "claimable_balances": next(iter(query_job2.result()))[0],
        "liquidity_pools": next(iter(query_job3.result()))[0],
        "offers": next(iter(query_job4.result()))[0],
        "trustlines": next(iter(query_job5.result()))[0],
        "config_settings": next(iter(query_job6.result()))[0],
        "contract_code": next(iter(query_job7.result()))[0],
        "contract_data": next(iter(query_job8.result()))[0],
        "ttl": next(iter(query_job9.result()))[0],
    }

    context["ti"].xcom_push(key="from BQ", value=BQ_results)
    context["ti"].xcom_push(key="from GCS", value=successful_transforms)

    # treating_errors(successful_transforms, BQ_results)


def get_from_historyTableExport(**context):
    successful_transforms = {
        "ledgers": 0,
        "operations": 0,
        "trades": 0,
        "effects": 0,
        "transactions": 0,
    }

    execution_date = context["execution_date"]
    yesterday = pendulum.instance(execution_date).subtract(days=2)
    # yesterday = pendulum.datetime(2024, 4, 24, 0, 0).in_timezone("UTC")

    print(yesterday)

    # Get the session from the settings
    session = settings.Session()

    # Get all the execution dates for the current date (yesterday)
    execution_dates = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "history_table_export",
            DagRun.execution_date >= yesterday.start_of("day"),
            DagRun.execution_date < yesterday.add(days=1).start_of("day"),
            DagRun.state == State.SUCCESS,
        )
        .all()
    )

    print(f"The len is :{len(execution_dates)}")

    dag_bag = DagBag()
    dag = dag_bag.get_dag("history_table_export")

    for key in successful_transforms.keys():
        task = dag.get_task("export_" + key + "_task")

        total_successful_transforms = 0

        for dag_run in execution_dates:
            # Retrieve successful_transforms from XCOM
            ti = TaskInstance(task, dag_run.execution_date)
            xcom_return = ti.xcom_pull(task_ids=task.task_id, key="return_value")

            successful_transforms_op = xcom_return["successful_transforms"]

            total_successful_transforms += successful_transforms_op

        successful_transforms[key] += total_successful_transforms

    # Query number of rows in BigQuery table
    query_job = do_query("crypto-stellar.crypto_stellar", "ledgers", yesterday, True)
    query_job1 = do_query(
        "crypto-stellar.crypto_stellar", "operations", yesterday, True
    )
    query_job2 = do_query("crypto-stellar.crypto_stellar", "trades", yesterday, True)
    query_job3 = do_query("crypto-stellar.crypto_stellar", "effects", yesterday, True)
    query_job4 = do_query(
        "crypto-stellar.crypto_stellar", "transactions", yesterday, True
    )

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

# compare_task = PythonOperator(
#    task_id="get_from_historyTableExport_task",
#    python_callable=get_from_historyTableExport,
#    provide_context=True,
#    dag=dag,
# )

# compare2_task = PythonOperator(
#    task_id="get_from_stateTables_task",
#    python_callable=get_from_stateTables,
#    provide_context=True,
#    dag=dag,
# )

compare_task3 = PythonOperator(
    task_id="comparison_task",
    python_callable=comparison,
    provide_context=True,
    dag=dag,
)
