from datetime import datetime, timedelta

# import pendulum
from airflow import DAG, settings
from airflow.models import DagBag, DagRun, TaskInstance, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.state import State
from google.cloud import bigquery
from google.oauth2 import service_account


def compare_transforms_and_bq_rows():
    # Try yesterday_ds again
    yesterday = datetime.now() - timedelta(days=1)
    # Get all the execution dates for the current date
    # Get the session from the settings
    session = settings.Session()

    # Get all the execution dates for the current date (yesterday)
    execution_dates = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "history_archive_without_captive_core",
            DagRun.execution_date >= yesterday.start_of("day"),
            DagRun.execution_date < yesterday.add(days=1).start_of("day"),
            DagRun.state == State.SUCCESS,
        )
        .all()
    )
    print(f"How many execution dates: {len(execution_dates)}")

    # Get the DAG
    dag_bag = DagBag()
    dag = dag_bag.get_dag("history_archive_without_captive_core")

    # Get the task
    task = dag.get_task("export_ledgers_task")

    total_successful_transforms = 0

    for dag_run in execution_dates:
        # Retrieve successful_transforms from XCOM
        ti = TaskInstance(task, dag_run.execution_date)
        xcom_ledgers = ti.xcom_pull(task_ids=task.task_id, key="return_value")

        # Parse JSON and get successful_transforms
        successful_transforms_ledgers = xcom_ledgers["successful_transforms"]
        total_successful_transforms += successful_transforms_ledgers

    print(f"Total successful transforms for yesterday: {total_successful_transforms}")

    key_path = Variable.get("api_key_path")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # # Query number of rows in BigQuery table
    query_job = client.query(
        """SELECT
        (SELECT COUNT(*) FROM crypto-stellar.crypto_stellar.history_ledgers
        WHERE DATE(batch_run_date)='2020-04-16') AS count_public
        """
    )
    results = query_job.result()
    # Convert the results to a list of rows
    rows = [dict(row) for row in results]

    # Print the number of rows in the BigQuery table
    print(f"in public:{rows[0]['count_public']}")

    # # Compare successful_transforms and bq_rows
    # if successful_transforms_ledgers != bq_rows:
    #     print("bq_rows are {0} and successful_transforms are {1}".format(bq_rows, successful_transforms_ledgers) )
    #     raise ValueError('Mismatch between successful_transforms in ledgers and bq_rows')


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
    task_id="compare_transforms_and_bq_rows",
    python_callable=compare_transforms_and_bq_rows,
    provide_context=True,
    dag=dag,
)
