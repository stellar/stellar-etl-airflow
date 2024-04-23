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
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "state_table_export",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 4, 23, 15, 0),
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
)

table_names = Variable.get("table_ids", deserialize_json=True)
internal_project = "{{ var.value.bq_project }}"
internal_dataset = "{{ var.value.bq_dataset }}"
public_project = "{{ var.value.public_project }}"
public_dataset = "{{ var.value.public_dataset }}"
use_testnet = literal_eval(Variable.get("use_testnet"))
use_futurenet = literal_eval(Variable.get("use_futurenet"))
use_captive_core = literal_eval(Variable.get("use_captive_core"))
txmeta_datastore_url = "{{ var.value.txmeta_datastore_url }}"

date_task = build_time_task(dag, use_testnet=use_testnet, use_futurenet=use_futurenet)
changes_task = build_export_task(
    dag,
    "bounded-core",
    "export_ledger_entry_changes",
    "{{ var.json.output_file_names.changes }}",
    use_testnet=use_testnet,
    use_futurenet=use_futurenet,
    use_gcs=True,
    use_captive_core=use_captive_core,
    txmeta_datastore_url=txmeta_datastore_url,
)

"""
The write batch stats task will take a snapshot of the DAG run_id, execution date,
start and end ledgers so that reconciliation and data validation are easier. The
record is written to an internal dataset for data eng use only.
"""
write_acc_stats = build_batch_stats(dag, table_names["accounts"])
write_bal_stats = build_batch_stats(dag, table_names["claimable_balances"])
write_off_stats = build_batch_stats(dag, table_names["offers"])
write_pool_stats = build_batch_stats(dag, table_names["liquidity_pools"])
write_sign_stats = build_batch_stats(dag, table_names["signers"])
write_trust_stats = build_batch_stats(dag, table_names["trustlines"])
write_contract_data_stats = build_batch_stats(dag, table_names["contract_data"])
write_contract_code_stats = build_batch_stats(dag, table_names["contract_code"])
write_config_settings_stats = build_batch_stats(dag, table_names["config_settings"])
write_ttl_stats = build_batch_stats(dag, table_names["ttl"])

"""
The delete partition task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.
"""
delete_acc_task = build_delete_data_task(
    dag, internal_project, internal_dataset, table_names["accounts"]
)
delete_acc_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["accounts"], "pub"
)
delete_bal_task = build_delete_data_task(
    dag, internal_project, internal_dataset, table_names["claimable_balances"]
)
delete_bal_pub_task = build_delete_data_task(
    dag,
    public_project,
    public_dataset,
    table_names["claimable_balances"],
    "pub",
)
delete_off_task = build_delete_data_task(
    dag, internal_project, internal_dataset, table_names["offers"]
)
delete_off_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["offers"], "pub"
)
delete_pool_task = build_delete_data_task(
    dag, internal_project, internal_dataset, table_names["liquidity_pools"]
)
delete_pool_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["liquidity_pools"], "pub"
)
delete_sign_task = build_delete_data_task(
    dag, internal_project, internal_dataset, table_names["signers"]
)
delete_sign_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["signers"], "pub"
)
delete_trust_task = build_delete_data_task(
    dag, internal_project, internal_dataset, table_names["trustlines"]
)
delete_trust_pub_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["trustlines"], "pub"
)
delete_contract_data_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["contract_data"], "pub"
)
delete_contract_code_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["contract_code"], "pub"
)
delete_config_settings_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["config_settings"], "pub"
)
delete_ttl_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["ttl"], "pub"
)

"""
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery.
Entries are updated, deleted, or inserted as needed.
"""
send_acc_to_bq_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    internal_project,
    internal_dataset,
    table_names["accounts"],
    "/*-accounts.txt",
    partition=True,
    cluster=True,
)
send_bal_to_bq_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    internal_project,
    internal_dataset,
    table_names["claimable_balances"],
    "/*-claimable_balances.txt",
    partition=True,
    cluster=True,
)
send_off_to_bq_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    internal_project,
    internal_dataset,
    table_names["offers"],
    "/*-offers.txt",
    partition=True,
    cluster=True,
)
send_pool_to_bq_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    internal_project,
    internal_dataset,
    table_names["liquidity_pools"],
    "/*-liquidity_pools.txt",
    partition=True,
    cluster=True,
)
send_sign_to_bq_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    internal_project,
    internal_dataset,
    table_names["signers"],
    "/*-signers.txt",
    partition=True,
    cluster=True,
)
send_trust_to_bq_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    internal_project,
    internal_dataset,
    table_names["trustlines"],
    "/*-trustlines.txt",
    partition=True,
    cluster=True,
)

"""
    Send to public dataset
"""
send_acc_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["accounts"],
    "/*-accounts.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_bal_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["claimable_balances"],
    "/*-claimable_balances.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_off_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["offers"],
    "/*-offers.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_pool_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["liquidity_pools"],
    "/*-liquidity_pools.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_sign_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["signers"],
    "/*-signers.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_trust_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["trustlines"],
    "/*-trustlines.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_contract_data_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["contract_data"],
    "/*-contract_data.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_contract_code_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["contract_code"],
    "/*-contract_code.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_config_settings_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["config_settings"],
    "/*-config_settings.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)
send_ttl_to_pub_task = build_gcs_to_bq_task(
    dag,
    changes_task.task_id,
    public_project,
    public_dataset,
    table_names["ttl"],
    "/*-ttl.txt",
    partition=True,
    cluster=True,
    dataset_type="pub",
)

date_task >> changes_task >> write_acc_stats >> delete_acc_task >> send_acc_to_bq_task
write_acc_stats >> delete_acc_pub_task >> send_acc_to_pub_task
date_task >> changes_task >> write_bal_stats >> delete_bal_task >> send_bal_to_bq_task
write_bal_stats >> delete_bal_pub_task >> send_bal_to_pub_task
date_task >> changes_task >> write_off_stats >> delete_off_task >> send_off_to_bq_task
write_off_stats >> delete_off_pub_task >> send_off_to_pub_task
(
    date_task
    >> changes_task
    >> write_pool_stats
    >> delete_pool_task
    >> send_pool_to_bq_task
)
write_pool_stats >> delete_pool_pub_task >> send_pool_to_pub_task
(
    date_task
    >> changes_task
    >> write_sign_stats
    >> delete_sign_task
    >> send_sign_to_bq_task
)
write_sign_stats >> delete_sign_pub_task >> send_sign_to_pub_task
(
    date_task
    >> changes_task
    >> write_trust_stats
    >> delete_trust_task
    >> send_trust_to_bq_task
)
write_trust_stats >> delete_trust_pub_task >> send_trust_to_pub_task
(
    date_task
    >> changes_task
    >> write_contract_data_stats
    >> delete_contract_data_task
    >> send_contract_data_to_pub_task
)
(
    date_task
    >> changes_task
    >> write_contract_code_stats
    >> delete_contract_code_task
    >> send_contract_code_to_pub_task
)
(
    date_task
    >> changes_task
    >> write_config_settings_stats
    >> delete_config_settings_task
    >> send_config_settings_to_pub_task
)
(
    date_task
    >> changes_task
    >> write_ttl_stats
    >> delete_ttl_task
    >> send_ttl_to_pub_task
)
