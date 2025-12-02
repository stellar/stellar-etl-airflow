"""
The state_table_export DAG exports ledger entry changes (accounts, offers, and trustlines) within a bounded range using stellar-core.
This DAG should be triggered manually if it is required to export entry changes within a specified time range.
"""

from ast import literal_eval
from datetime import datetime
from json import loads

from airflow import DAG
from airflow.models import Variable
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_del_ins_from_gcs_to_bq_task import (
    build_del_ins_from_gcs_to_bq_task,
)
from stellar_etl_airflow.build_del_ins_operator import (
    create_del_ins_task,
    initialize_task_vars,
)
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

# Initialize the DAG
dag = DAG(
    "state_table_export",
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 9, 13, 15, 0),
    description="This DAG runs a bounded stellar-core instance, which allows it to export accounts, offers, liquidity pools, and trustlines to BigQuery.",
    schedule_interval="*/10 * * * *",
    params={
        "alias": "state",
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
    catchup=True,
    # sla_miss_callback=alert_sla_miss,
)

# Initialize batch metadata variables
batch_id = macros.get_batch_id()
batch_date = "{{ batch_run_date_as_datetime_string(dag, data_interval_start) }}"

# Fetch necessary variables
table_names = Variable.get("table_ids", deserialize_json=True)
internal_project = "{{ var.value.bq_project }}"
internal_dataset = "{{ var.value.bq_dataset }}"
public_project = "{{ var.value.public_project }}"
public_dataset = "{{ var.value.public_dataset }}"
use_testnet = literal_eval(Variable.get("use_testnet"))
use_futurenet = literal_eval(Variable.get("use_futurenet"))
use_captive_core = literal_eval(Variable.get("use_captive_core"))
txmeta_datastore_path = "{{ var.value.txmeta_datastore_path }}"

# Ensure all required keys are present in table_names
required_keys = [
    "accounts",
    "claimable_balances",
    "offers",
    "liquidity_pools",
    "signers",
    "trustlines",
    "contract_data",
    "contract_code",
    "config_settings",
    "ttl",
    "restored_key",
]

missing_keys = [key for key in required_keys if key not in table_names]
if missing_keys:
    raise KeyError(f"Missing Id in the table_ids Airflow Variable: {missing_keys}")

changes_task = build_export_task(
    dag,
    "bounded-core",
    "export_ledger_entry_changes",
    "{{ var.json.output_file_names.changes }}",
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

The Insert part of the task receives the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in the public dataset.
Entries are updated, deleted, or inserted as needed.
"""
del_ins_tasks = {}

# Define the suffixes for the DAG related tables
source_object_suffix_mapping = {
    "accounts": "/changes_folder/*-accounts.txt",
    "claimable_balances": "/changes_folder/*-claimable_balances.txt",
    "offers": "/changes_folder/*-offers.txt",
    "liquidity_pools": "/changes_folder/*-liquidity_pools.txt",
    "signers": "/changes_folder/*-signers.txt",
    "trustlines": "/changes_folder/*-trustlines.txt",
    "contract_data": "/changes_folder/*-contract_data.txt",
    "contract_code": "/changes_folder/*-contract_code.txt",
    "config_settings": "/changes_folder/*-config_settings.txt",
    "ttl": "/changes_folder/*-ttl.txt",
    "restored_key": "/changes_folder/*-restored_key.txt",
}

for table_id, source_object_suffix in source_object_suffix_mapping.items():
    table_name = table_names[table_id]  # Get the expanded table name
    task_vars = initialize_task_vars(
        table_id,
        table_name,
        changes_task.task_id,
        batch_id,
        batch_date,
        public_project,
        public_dataset,
        source_object_suffix=source_object_suffix,
    )
    del_ins_tasks[table_id] = create_del_ins_task(
        dag, task_vars, build_del_ins_from_gcs_to_bq_task
    )

# Set task dependencies
(changes_task >> del_ins_tasks["accounts"])
(changes_task >> del_ins_tasks["claimable_balances"])
(changes_task >> del_ins_tasks["offers"])
(changes_task >> del_ins_tasks["liquidity_pools"])
(changes_task >> del_ins_tasks["signers"])
(changes_task >> del_ins_tasks["trustlines"])
(
    changes_task
    >> del_ins_tasks["contract_data"]
)
(
    changes_task
    >> del_ins_tasks["contract_code"]
)
(
    changes_task
    >> del_ins_tasks["config_settings"]
)
(changes_task >> del_ins_tasks["ttl"])
(
    changes_task
    >> del_ins_tasks["restored_key"]
)
