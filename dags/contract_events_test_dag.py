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
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()


dag = DAG(
    "contract_events_test",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 6, 18, 1, 0),
    catchup=True,
    description="This DAG exports contract events.",
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
    sla_miss_callback=alert_sla_miss,
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
The write batch stats task will take a snapshot of the DAG run_id, execution date,
start and end ledgers so that reconciliation and data validation are easier. The
record is written to an internal dataset for data eng use only.
"""
write_contract_events_stats = build_batch_stats(dag, "contract_events")


"""
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the command are stored in a file. There is one task for each of the data types that
can be exported from the history archives.


The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task
starts prematurely and will not load data.
"""
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


"""
The delete partition task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.
"""


delete_contract_events_task = build_delete_data_task(
    dag, public_project, public_dataset, table_names["contract_events"], "pub"
)


"""
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery.
"""

"""
Load final public dataset, crypto-stellar
"""

send_contract_events_to_pub_task = build_gcs_to_bq_task(
    dag,
    contract_events_export_task.task_id,
    public_project,
    public_dataset,
    table_names["contract_events"],
    "",
    partition=True,
    cluster=True,
    dataset_type="pub",
)



(
    time_task
    >> write_contract_events_stats
    >> contract_events_export_task
    >> delete_contract_events_task
    >> send_contract_events_to_pub_task
)
