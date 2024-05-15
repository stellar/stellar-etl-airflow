"""
The history_archive_export DAG exports operations and trades from the history archives.
It is scheduled to export information to BigQuery at regular intervals.
"""
from ast import literal_eval
from datetime import datetime
from json import loads

from airflow import DAG
from airflow.models.variable import Variable
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_bq_insert_job_task import build_bq_insert_job
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()


dag = DAG(
    "soroban_backfill_2",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 5, 11),
    end_date=datetime(2024, 5, 12),
    catchup=True,
    description="This DAG exports trades and operations from the history archive using CaptiveCore. This supports parsing sponsorship and AMMs.",
    schedule_interval="@daily",
    params={
        "alias": "backfill",
    },
    render_template_as_native_obj=True,
    user_defined_filters={
        "fromjson": lambda s: loads(s),
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
        "literal_eval": lambda e: literal_eval(e),
    },
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
)


table_names = Variable.get("table_ids", deserialize_json=True)
internal_project = "{{ var.value.bq_project }}"
internal_dataset = "{{ var.value.bq_dataset }}"
public_project = "{{ var.value.public_project }}"
public_dataset = "test_crypto_stellar_backfill"
use_testnet = literal_eval(Variable.get("use_testnet"))
use_futurenet = literal_eval(Variable.get("use_futurenet"))
use_captive_core = literal_eval(Variable.get("use_captive_core"))
txmeta_datastore_path = "{{ var.value.txmeta_datastore_path }}"


"""
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
"""
time_task = build_time_task(dag, use_testnet=use_testnet, use_futurenet=use_futurenet)


"""
The write batch stats task will take a snapshot of the DAG run_id, execution date,
start and end ledgers so that reconciliation and data validation are easier. The
record is written to an internal dataset for data eng use only.
"""
write_op_stats = build_batch_stats(dag, table_names["operations"])
write_tx_stats = build_batch_stats(dag, table_names["transactions"])
write_ledger_stats = build_batch_stats(dag, table_names["ledgers"])


"""
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the command are stored in a file. There is one task for each of the data types that
can be exported from the history archives.


The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task
starts prematurely and will not load data.
"""
op_export_task = build_export_task(
    dag,
    "archive",
    "export_operations",
    "{{ var.json.output_file_names.operations }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_path=txmeta_datastore_path,
)

tx_export_task = build_export_task(
    dag,
    "archive",
    "export_transactions",
    "{{ var.json.output_file_names.transactions }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_path=txmeta_datastore_path,
)

ledger_export_task = build_export_task(
    dag,
    "archive",
    "export_ledgers",
    "{{ var.json.output_file_names.ledgers }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_path=txmeta_datastore_path,
)




"""
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery.
"""

"""
Load final public dataset, crypto-stellar
"""
send_ops_to_pub_task = build_gcs_to_bq_task(
    dag,
    op_export_task.task_id,
    public_project,
    public_dataset,
    table_names["operations"],
    "",
    partition=True,
    cluster=True,
    dataset_type="pub",
)

send_txs_to_pub_task = build_gcs_to_bq_task(
    dag,
    tx_export_task.task_id,
    public_project,
    public_dataset,
   table_names["transactions"],
    "",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_ledgers_to_pub_task = build_gcs_to_bq_task(
    dag,
    ledger_export_task.task_id,
    public_project,
    public_dataset,
    table_names["ledgers"],
    "",
    partition=True,
    cluster=True,
    dataset_type="pub",
)


(
    time_task
    >> write_op_stats
    >> op_export_task
    >> send_ops_to_pub_task

)



(
    time_task
    >> write_tx_stats
    >> tx_export_task
    >> send_txs_to_pub_task
)



(
    time_task
    >> write_ledger_stats
    >> ledger_export_task
    >> send_ledgers_to_pub_task
)
