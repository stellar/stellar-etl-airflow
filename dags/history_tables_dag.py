"""
The history_archive_export DAG exports trades, asstes, ledgers, operations, transactions, effects from the history archives.
It is scheduled to export information to BigQuery at regular intervals.
"""

from ast import literal_eval
from datetime import datetime
from json import loads

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_bq_insert_job_task import build_bq_insert_job
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_del_ins_from_gcs_to_bq_task import (
    build_del_ins_from_gcs_to_bq_task,
)
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import (
    alert_after_max_retries,
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

# Initialize the DAG
dag = DAG(
    "history_tables_export",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 7, 8, 16, 00),
    catchup=True,
    description="This DAG exports trades, asstes, ledgers, operations, transactions, effects from the history archive using CaptiveCore. This supports parsing sponsorship and AMMs.",
    schedule_interval="*/10 * * * *",
    params={
        "alias": "cc",
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
    # sla_miss_callback=alert_sla_miss,
)

# fetch and set batch metadata variables
batch_id = macros.get_batch_id()
batch_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"

source_object_suffix = ""

# Fetch necessary variables
table_names = Variable.get("table_ids", deserialize_json=True)
public_project = "{{ var.value.public_project }}"
public_dataset = "{{ var.value.public_dataset }}"
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
write_trade_stats = build_batch_stats(dag, table_names["trades"])
write_effects_stats = build_batch_stats(dag, table_names["effects"])
write_tx_stats = build_batch_stats(dag, table_names["transactions"])
write_ledger_stats = build_batch_stats(dag, table_names["ledgers"])
write_asset_stats = build_batch_stats(dag, table_names["assets"])
write_contract_events_stats = build_batch_stats(dag, "contract_events")


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
trade_export_task = build_export_task(
    dag,
    "archive",
    "export_trades",
    "{{ var.json.output_file_names.trades }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_path=txmeta_datastore_path,
)
effects_export_task = build_export_task(
    dag,
    "archive",
    "export_effects",
    "effects.txt",
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
asset_export_task = build_export_task(
    dag,
    "archive",
    "export_assets",
    "{{ var.json.output_file_names.assets }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_path=txmeta_datastore_path,
)
contract_events_export_task = build_export_task(
    dag,
    "archive",
    "export_contract_events",
    "{{ var.json.output_file_names.contract_events }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_path=txmeta_datastore_path,
)


# define the variables needed for making the call to the Operator that does the Delete from BQ Table and Inserts data from GCS to BigQuery

ops_export_task_id = op_export_task.task_id
trades_export_task_id = trade_export_task.task_id
effects_export_task_id = effects_export_task.task_id
txns_export_task_id = tx_export_task.task_id
ledgers_export_task_id = ledger_export_task.task_id
assets_export_task_id = asset_export_task.task_id
cont_evnts_export_task_id = contract_events_export_task.task_id

ops_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + ops_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

trade_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + trades_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

effects_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + effects_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

txns_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + txns_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

ledger_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + ledgers_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

asset_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + assets_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

cont_evnts_source_objects = [
    "{{ task_instance.xcom_pull(task_ids='"
    + cont_evnts_export_task_id
    + '\')["output"] }}'
    + source_object_suffix
]

"""
The task calls the Custom Python Task that does the following operations in sequence
    1. The delete operation in the task checks to see if the given partition/batch id exists in
       Bigquery. If it does, the records are deleted prior to reinserting the batch.
    2. The Insert operation in the tasks receive the location of the file in Google Cloud storage
    through Airflow's XCOM system and merges the unique entries in the file into the corresponding table in BigQuery.
"""

del_ins_ops_task = PythonOperator(
    task_id=f"del_ins_ops_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["operations"],
        ops_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        ops_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

del_ins_trades_task = PythonOperator(
    task_id=f"del_ins_trades_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["trades"],
        trades_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        trade_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

del_ins_effects_task = PythonOperator(
    task_id=f"del_ins_effects_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["effects"],
        effects_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        effects_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

del_ins_txns_task = PythonOperator(
    task_id=f"del_ins_txns_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["transactions"],
        txns_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        txns_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

del_ins_ledgers_task = PythonOperator(
    task_id=f"del_ins_ledgers_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["ledgers"],
        ledgers_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        ledger_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

del_ins_assets_task = PythonOperator(
    task_id=f"del_ins_assets_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["assets"],
        assets_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        asset_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

del_ins_cont_events_task = PythonOperator(
    task_id=f"del_ins_cont_evnts_task",
    python_callable=build_del_ins_from_gcs_to_bq_task,
    op_args=[
        public_project,
        public_dataset,
        table_names["contract_events"],
        cont_evnts_export_task_id,
        source_object_suffix,
        True,
        True,
        batch_id,
        batch_date,
        cont_evnts_source_objects,
    ],
    provide_context=True,
    dag=dag,
)

"""
The delete partition task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.
"""
delete_enrich_op_pub_task = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    "enriched_history_operations",
    "pub",
)

"""
The task handles the Insert operation into BQ for EHO and assets tables
"""

insert_enriched_hist_pub_task = build_bq_insert_job(
    dag,
    public_project,
    public_dataset,
    "enriched_history_operations",
    partition=True,
    cluster=True,
    dataset_type="pub",
)

dedup_assets_pub_task = build_bq_insert_job(
    dag,
    public_project,
    public_dataset,
    table_names["assets"],
    partition=True,
    cluster=True,
    create=True,
    dataset_type="pub",
)

# Set Task dependencies

(
    time_task
    >> write_op_stats
    >> op_export_task
    >> del_ins_ops_task
    >> delete_enrich_op_pub_task
    >> insert_enriched_hist_pub_task
)

(time_task >> write_trade_stats >> trade_export_task >> del_ins_trades_task)

(time_task >> write_effects_stats >> effects_export_task >> del_ins_effects_task)

(
    time_task
    >> write_tx_stats
    >> tx_export_task
    >> del_ins_txns_task
    >> delete_enrich_op_pub_task
)

(
    time_task
    >> write_ledger_stats
    >> ledger_export_task
    >> del_ins_ledgers_task
    >> delete_enrich_op_pub_task
)

(
    time_task
    >> write_asset_stats
    >> asset_export_task
    >> del_ins_assets_task
    >> dedup_assets_pub_task
)

(
    time_task
    >> write_contract_events_stats
    >> contract_events_export_task
    >> del_ins_cont_events_task
)
