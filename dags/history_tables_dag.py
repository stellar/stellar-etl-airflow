"""
The history_table_export DAG exports trades, assets, ledgers, operations, transactions, effects, and contract events
from the history archives and loads the data into the corresponding BigQuery tables. It also performs a Delete operation
based on the batch interval value to perform clean up before the Insert to avoid any potential duplication issues.
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
from stellar_etl_airflow.build_del_ins_from_gcs_to_bq_task import (
    build_del_ins_from_gcs_to_bq_task,
)
from stellar_etl_airflow.build_del_ins_operator import (
    create_del_ins_task,
    initialize_task_vars,
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
    "history_table_export",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 7, 10, 14, 30),
    catchup=True,
    description="This DAG exports information for the trades, assets, ledgers, operations, transactions, effects and contract events history tables.",
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

# Initialize batch metadata variables
batch_id = macros.get_batch_id()
batch_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"

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
The build batch stats task will take a snapshot of the DAG run_id, execution date,
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
    resource_cfg="stellaretl",
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
    resource_cfg="stellaretl",
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
    resource_cfg="stellaretl",
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
    resource_cfg="stellaretl",
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
    resource_cfg="stellaretl",
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
    resource_cfg="stellaretl",
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
    resource_cfg="stellaretl",
)

"""
The delete part of the task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.

The Insert part of the task receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery.

"""
del_ins_tasks = {}

export_tasks = {
    "operations": op_export_task,
    "trades": trade_export_task,
    "effects": effects_export_task,
    "transactions": tx_export_task,
    "ledgers": ledger_export_task,
    "assets": asset_export_task,
    "contract_events": contract_events_export_task,
}


for table_id, export_task in export_tasks.items():
    table_name = table_names[table_id]
    task_vars = initialize_task_vars(
        table_id,
        table_name,
        export_task.task_id,
        batch_id,
        batch_date,
        public_project,
        public_dataset,
    )
    del_ins_tasks[table_id] = create_del_ins_task(
        dag, task_vars, build_del_ins_from_gcs_to_bq_task
    )

"""
Delete and Insert operations for the Enriched History Operations table
"""

delete_enrich_op_pub_task = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    "enriched_history_operations",
    "pub",
)

insert_enrich_op_pub_task = build_bq_insert_job(
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
    >> del_ins_tasks["operations"]
    >> delete_enrich_op_pub_task
    >> insert_enrich_op_pub_task
)

(time_task >> write_trade_stats >> trade_export_task >> del_ins_tasks["trades"])
(time_task >> write_effects_stats >> effects_export_task >> del_ins_tasks["effects"])
(
    time_task
    >> write_tx_stats
    >> tx_export_task
    >> del_ins_tasks["transactions"]
    >> delete_enrich_op_pub_task
)
(
    time_task
    >> write_ledger_stats
    >> ledger_export_task
    >> del_ins_tasks["ledgers"]
    >> delete_enrich_op_pub_task
)
(
    time_task
    >> write_asset_stats
    >> asset_export_task
    >> del_ins_tasks["assets"]
    >> dedup_assets_pub_task
)
(
    time_task
    >> write_contract_events_stats
    >> contract_events_export_task
    >> del_ins_tasks["contract_events"]
)
