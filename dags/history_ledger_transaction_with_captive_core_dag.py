"""
The history_archive_export DAG exports ledger_transactions from the history archives.
It is scheduled to export information to BigQuery at regular intervals.
"""
import ast
import datetime
import json

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_date_lake_to_bq_task import build_data_lake_to_bq_task
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_export_to_lake_task import export_to_lake
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "history_ledger_transaction_with_captive_core",
    default_args=get_default_dag_args(),
    catchup=False,
    start_date=datetime.datetime(2023, 10, 24, 13, 0),
    description="This DAG exports ledger transaction from the history archive using CaptiveCore. This supports parsing sponsorship and AMMs.",
    schedule_interval="*/30 * * * *",
    params={
        "alias": "cc",
    },
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
)

file_names = Variable.get("output_file_names", deserialize_json=True)
table_names = Variable.get("table_ids", deserialize_json=True)
internal_project = Variable.get("bq_project")
internal_dataset = Variable.get("bq_dataset")
public_project = Variable.get("public_project")
public_dataset = Variable.get("public_dataset")
use_testnet = ast.literal_eval(Variable.get("use_testnet"))


"""
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
"""
time_task = build_time_task(dag, use_testnet=use_testnet)

"""
The write batch stats task will take a snapshot of the DAG run_id, execution date,
start and end ledgers so that reconciliation and data validation are easier. The
record is written to an internal dataset for data eng use only.
"""

write_lt_stats = build_batch_stats(dag, table_names["ledger_transaction"])

"""
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the command are stored in a file. There is one task for each of the data types that
can be exported from the history archives.

The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task
starts prematurely and will not load data.
"""


lt_export_task = build_export_task(
    dag,
    "archive",
    "export_ledger_transaction",
    file_names["ledger_transaction"],
    use_testnet=use_testnet,
    use_gcs=True,
    resource_cfg="cc",
)

ledger_range = "{{ task_instance.xcom_pull(task_ids='time_task.task_id') }}"
ledger_range = ast.literal_eval(ledger_range)
for ledger in range(ledger_range["start"], ledger_range["end"]):
    lt_lake_export_task = export_to_lake(dag, lt_export_task.task_id, ledger)

lt_bq_task = build_data_lake_to_bq_task(
    dag,
    internal_project,
    internal_dataset,
    "ledger_transaction",
    ledger_range,
)

(time_task >> write_lt_stats >> lt_export_task >> lt_lake_export_task >> lt_bq_task)
