'''
The history_archive_export DAG exports ledgers and transactions from the history archives. 
It is scheduled to export information to BigQuery at regular intervals.
'''
import ast
import datetime
import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow.build_bq_insert_job_task import build_bq_insert_job
from stellar_etl_airflow import macros

from airflow import DAG
from airflow.models import Variable

init_sentry()

dag = DAG(
    'history_archive_without_captive_core',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2022, 3, 11, 18, 30),
    description='This DAG exports ledgers, transactions, and assets from the history archive to BigQuery. Incremental Loads',
    schedule_interval='*/15 * * * *',
    params={
        'alias': 'archive',
    },
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
    user_defined_macros={
        'subtract_data_interval': macros.subtract_data_interval,
        'batch_run_date_as_datetime_string': macros.batch_run_date_as_datetime_string,
    },
)

file_names = Variable.get('output_file_names', deserialize_json=True)
table_names = Variable.get('table_ids', deserialize_json=True)
internal_project = Variable.get('bq_project')
internal_dataset = Variable.get('bq_dataset')
public_project = Variable.get('public_project')
public_dataset = Variable.get('public_dataset')
use_testnet = ast.literal_eval(Variable.get("use_testnet"))

'''
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
'''
time_task = build_time_task(dag, use_testnet=use_testnet)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date, 
start and end ledgers so that reconciliation and data validation are easier. The 
record is written to an internal dataset for data eng use only.
'''
write_ledger_stats = build_batch_stats(dag, table_names['ledgers'])
write_tx_stats = build_batch_stats(dag, table_names['transactions'])
write_asset_stats = build_batch_stats(dag, table_names['assets'])

'''
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the command are stored in a file. There is one task for each of the data types that
can be exported from the history archives.

The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task 
starts prematurely and will not load data.
'''
ledger_export_task = build_export_task(dag, 'archive', 'export_ledgers', file_names['ledgers'], use_testnet=use_testnet, use_gcs=True)
tx_export_task = build_export_task(dag, 'archive', 'export_transactions', file_names['transactions'], use_testnet=use_testnet, use_gcs=True)
asset_export_task = build_export_task(dag, 'archive', 'export_assets', file_names['assets'], use_testnet=use_testnet, use_gcs=True)

'''
The delete partition task checks to see if the given partition/batch id exists in 
Bigquery. If it does, the records are deleted prior to reinserting the batch.
'''
delete_old_ledger_task = build_delete_data_task(dag, internal_project, internal_dataset, table_names['ledgers'])
delete_old_ledger_pub_task = build_delete_data_task(dag, public_project, public_dataset, table_names['ledgers'])
delete_old_tx_task = build_delete_data_task(dag, internal_project, internal_dataset, table_names['transactions'])
delete_old_tx_pub_task = build_delete_data_task(dag, public_project, public_dataset, table_names['transactions'])
delete_old_asset_task = build_delete_data_task(dag, internal_project, internal_dataset, table_names['assets'])
delete_old_asset_pub_task = build_delete_data_task(dag, public_project, public_dataset, table_names['assets'])

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ledgers_to_bq_task = build_gcs_to_bq_task(dag, ledger_export_task.task_id, internal_project, internal_dataset, table_names['ledgers'], '', partition=True, cluster=False)
send_txs_to_bq_task = build_gcs_to_bq_task(dag, tx_export_task.task_id, internal_project, internal_dataset, table_names['transactions'], '', partition=True, cluster=False)
send_assets_to_bq_task = build_gcs_to_bq_task(dag, asset_export_task.task_id, internal_project, internal_dataset, table_names['assets'], '', partition=False, cluster=False)

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ledgers_to_pub_task = build_gcs_to_bq_task(dag, ledger_export_task.task_id, public_project, public_dataset, table_names['ledgers'], '', partition=True, cluster=True)
send_txs_to_pub_task = build_gcs_to_bq_task(dag, tx_export_task.task_id, public_project, public_dataset, table_names['transactions'], '', partition=True, cluster=True)
send_assets_to_pub_task = build_gcs_to_bq_task(dag, asset_export_task.task_id, public_project, public_dataset, table_names['assets'], '', partition=True, cluster=True)

'''
The tasks below use a job in BigQuery to deduplicate the table history_assets_stg.
The job refreshes the table history_assets with only new records.
'''
dedup_assets_bq_task = build_bq_insert_job(dag, internal_project, internal_dataset, table_names['assets'], partition=False)
dedup_assets_pub_task = build_bq_insert_job(dag, public_project, public_dataset, table_names['assets'], partition=True)

time_task >> write_ledger_stats >> ledger_export_task >> delete_old_ledger_task >> send_ledgers_to_bq_task
ledger_export_task >> delete_old_ledger_pub_task >> send_ledgers_to_pub_task
time_task >> write_tx_stats >> tx_export_task >> delete_old_tx_task >> send_txs_to_bq_task
tx_export_task >> delete_old_tx_pub_task >> send_txs_to_pub_task
time_task >> write_asset_stats >> asset_export_task  >> delete_old_asset_task >> send_assets_to_bq_task >> dedup_assets_bq_task
asset_export_task >> delete_old_asset_pub_task >> send_assets_to_pub_task >> dedup_assets_pub_task