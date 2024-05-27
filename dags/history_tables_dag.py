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
    "history_table_export",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 4, 23, 15, 0),
    catchup=True,
    description="This DAG exports trades and operations from the history archive using CaptiveCore. This supports parsing sponsorship and AMMs.",
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
)

table_names = Variable.get("table_ids", deserialize_json=True)
internal_project = "{{ var.value.bq_project }}"
internal_dataset = "{{ var.value.bq_dataset }}"
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
diagnostic_events_export_task = build_export_task(
    dag,
    "archive",
    "export_diagnostic_events",
    "{{ var.json.output_file_names.diagnostic_events }}",
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


"""
The delete partition task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.
"""


delete_old_op_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["operations"], "pub"
)


delete_old_trade_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["trades"], "pub"
)


delete_enrich_op_pub_task = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    "enriched_history_operations",
    "pub",
)


delete_old_effects_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["effects"], "pub"
)


delete_old_tx_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["transactions"], "pub"
)


delete_old_ledger_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["ledgers"], "pub"
)

delete_old_asset_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["assets"], "pub"
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
send_trades_to_pub_task = build_gcs_to_bq_task(
    dag,
    trade_export_task.task_id,
    public_project,
    public_dataset,
    table_names["trades"],
    "",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_effects_to_pub_task = build_gcs_to_bq_task(
    dag,
    effects_export_task.task_id,
    public_project,
    public_dataset,
    table_names["effects"],
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


send_assets_to_pub_task = build_gcs_to_bq_task(
    dag,
    asset_export_task.task_id,
    public_project,
    public_dataset,
    table_names["assets"],
    "",
    partition=True,
    cluster=True,
    dataset_type="pub",
)


insert_enriched_hist_pub_task = build_bq_insert_job(
    dag,
    public_project,
    public_dataset,
    "enriched_history_operations",
    partition=True,
    cluster=True,
    dataset_type="pub",
)


(
    time_task
    # >> op_export_task
    # >> delete_old_op_pub_task
    # >> send_ops_to_pub_task
    # >> delete_enrich_op_pub_task
    # >> insert_enriched_hist_pub_task
)


# (time_task >> trade_export_task >> delete_old_trade_pub_task >> send_trades_to_pub_task)


# (
#     time_task
#     >> effects_export_task
#     >> delete_old_effects_pub_task
#     >> send_effects_to_pub_task
# )


# (
#     time_task
#     >> tx_export_task
#     >> delete_old_tx_pub_task
#     >> send_txs_to_pub_task
#     >> delete_enrich_op_pub_task
# )
# (time_task >> diagnostic_events_export_task)
# (
#     [
#         insert_enriched_hist_pub_task,
#     ]
# )

# dedup_assets_pub_task = build_bq_insert_job(
#     dag,
#     public_project,
#     public_dataset,
#     table_names["assets"],
#     partition=True,
#     cluster=True,
#     create=True,
#     dataset_type="pub",
# )
# (
#     time_task
#     >> ledger_export_task
#     >> delete_old_ledger_pub_task
#     >> send_ledgers_to_pub_task
#     >> delete_enrich_op_pub_task
# )
# (
#     time_task
#     >> asset_export_task
#     >> delete_old_asset_pub_task
#     >> send_assets_to_pub_task
#     >> dedup_assets_pub_task
# )
