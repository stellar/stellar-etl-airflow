from datetime import datetime, time, timedelta

import pendulum
import pytz
from airflow import DAG, settings
from airflow.models import DagBag, DagRun, TaskInstance, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.state import State
from google.cloud import bigquery
from google.oauth2 import service_account


def treating_errors(successful_transforms, BQ_results):
    if successful_transforms["operations"] != BQ_results["operations"]:
        print(
            "bq_operations are {0} and successful_transforms operations are {1}".format(
                BQ_results["operations"], successful_transforms["operations"]
            )
        )
        raise ValueError("Mismatch between operations in GCS and BQ operations")
    elif successful_transforms["trades"] != BQ_results["trades"]:
        print(
            "bq_operations are {0} and successful_transforms operations are {1}".format(
                BQ_results["trades"], successful_transforms["trades"]
            )
        )
        raise ValueError("Mismatch between trades in GCS and BQ trades")
    elif successful_transforms["effects"] != BQ_results["effects"]:
        print(
            "bq_operations are {0} and successful_transforms operations are {1}".format(
                BQ_results["effects"], successful_transforms["effects"]
            )
        )
        raise ValueError("Mismatch between effects in GCS and BQ effects")
    elif successful_transforms["transactions"] != BQ_results["transactions"]:
        print(
            "bq_operations are {0} and successful_transforms operations are {1}".format(
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


def get_from_combinedExport(**context):
    successful_transforms = {
        "operations": 0,
        "trades": 0,
        "effects": 0,
        "transactions": 0,
    }

    yesterday = pendulum.datetime(2024, 4, 16, tz="UTC")

    session = settings.Session()

    # Get all the execution dates for the current date (yesterday)
    execution_dates = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "history_archive_with_captive_core_combined_export",
            DagRun.execution_date >= yesterday.start_of("day"),
            DagRun.execution_date < yesterday.add(days=1).start_of("day"),
            DagRun.state == State.SUCCESS,
        )
        .all()
    )

    gcs_hook = GCSHook(google_cloud_storage_conn_id="google_cloud_storage_default")

    print(f"how many runs: {len(execution_dates)}")

    for dag_run in execution_dates:
        execution_date_str = dag_run.execution_date.strftime(
            "%Y-%m-%d %H:%M:%S%z"
        ).replace(" ", "T")
        execution_date_str = execution_date_str[:-2] + ":" + execution_date_str[-2:]

        # Download the file and get its content, it runs 47 times day 16th of april
        file_content = gcs_hook.download(
            bucket_name="us-central1-test-hubble-2-5f1f2dbf-bucket",
            object_name=f"logs/dag_id=history_archive_with_captive_core_combined_export/run_id=scheduled__{execution_date_str}/task_id=export_all_history_task/attempt=1.log",
        )

        # Decode the bytes object to a string
        file_content = file_content.decode()

        # Now file_content is a string with the content of the file
        lines = file_content.splitlines()

        successful_values = []

        for line in lines:
            if 'level=info msg="{\\' in line:
                start = line.find('{\\"')
                # Slice the line from the start of the JSON string
                # Find the start and end of the JSON string
                end = line.rfind("}") + 1  # +1 to include the '}' character

                # Slice the string from the start to the end of the JSON string
                json_str = line[start:end]

                # Find the last colon and the closing brace in the string
                last_colon = json_str.rfind(":")
                closing_brace = json_str.rfind("}")

                # Slice the string to get the value between the last colon and the closing brace
                value = json_str[last_colon + 1 : closing_brace]

                successful_values.append(int(value))

        for key, val in zip(successful_transforms, successful_values):
            successful_transforms[key] += val

    query_job = do_query("operations", yesterday)
    query_job2 = do_query("trades", yesterday)
    query_job3 = do_query("effects", yesterday)
    query_job4 = do_query("transactions", yesterday)

    BQ_results = {
        "operations": next(iter(query_job.result()))[0],
        "trades": next(iter(query_job2.result()))[0],
        "effects": next(iter(query_job3.result()))[0],
        "transactions": next(iter(query_job4.result()))[0],
    }

    context["ti"].xcom_push(key="from BQ", value=BQ_results)
    context["ti"].xcom_push(key="from GCS", value=successful_transforms)

    treating_errors(successful_transforms, BQ_results)


def get_from_without_captiveCore(**context):
    # execution_date = context["execution_date"]
    # yesterday = pendulum.instance(execution_date).subtract(days=1)
    # yesterday = datetime.combine(yesterday, time(), tzinfo=pytz.timezone("UTC"))
    yesterday = pendulum.datetime(2024, 4, 16, tz="UTC")

    # Get the session from the settings
    session = settings.Session()

    # Get all the execution dates for the current date (yesterday)
    execution_dates = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "history_archive_without_captive_core",
            DagRun.execution_date >= yesterday,
            DagRun.execution_date
            < datetime.combine(
                yesterday + timedelta(days=1), time(), tzinfo=pytz.timezone("UTC")
            ),
            DagRun.state == State.SUCCESS,
        )
        .all()
    )

    dag_bag = DagBag()
    dag = dag_bag.get_dag("history_archive_without_captive_core")

    task = dag.get_task("export_ledgers_task")

    total_successful_transforms = 0

    for dag_run in execution_dates:
        # Retrieve successful_transforms from XCOM
        ti = TaskInstance(task, dag_run.execution_date)
        xcom_ledgers = ti.xcom_pull(task_ids=task.task_id, key="return_value")

        # Parse JSON and get successful_transforms
        successful_transforms_ledgers = xcom_ledgers["successful_transforms"]
        total_successful_transforms += successful_transforms_ledgers

    # # Query number of rows in BigQuery table
    query_job = do_query("ledgers", yesterday)

    results = query_job.result()

    rows = [dict(row) for row in results]

    context["ti"].xcom_push(key="from BQ", value=rows[0])
    context["ti"].xcom_push(key="from GCS", value=total_successful_transforms)

    # Compare successful_transforms and bq_rows
    if total_successful_transforms != rows[0]:
        print(
            "bq_rows are {0} and successful_transforms are {1}".format(
                rows[0]["count_public"], total_successful_transforms
            )
        )
        raise ValueError(
            "Mismatch between successful_transforms in ledgers and bq_rows"
        )


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
    task_id="get_from_without_captiveCore",
    python_callable=get_from_without_captiveCore,
    provide_context=True,
    dag=dag,
)

compare2_task = PythonOperator(
    task_id="get_from_combinedExport",
    python_callable=get_from_combinedExport,
    provide_context=True,
    dag=dag,
)
