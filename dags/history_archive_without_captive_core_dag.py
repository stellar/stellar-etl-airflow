'''
The history_archive_export DAG exports ledgers and transactions from the history archives. 
It is scheduled to export information to BigQuery at regular intervals.
'''
import ast
import datetime
import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

from airflow import DAG
from airflow.models import Variable


dag = DAG(
    'history_archive_without_captive_core',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2021, 11, 29, 22, 30),
    description='This DAG exports ledgers, transactions, and assets from the history archive to BigQuery. Incremental Loads',
    schedule_interval='*/6 * * * *',
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)
use_testnet = ast.literal_eval(Variable.get("use_testnet"))

'''
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
'''
time_task = build_time_task(dag)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date, 
start and end ledgers so that reconciliation and data validation are easier. The 
record is written to an internal dataset for data eng use only.
'''
write_ledger_stats = build_batch_stats(dag, 'history_ledgers')
write_tx_stats = build_batch_stats(dag, 'history_transactions')
write_asset_stats = build_batch_stats(dag, 'history_assets')

'''
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the comand are stored in a file. There is one task for each of the data types that 
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
delete_old_ledger_task = build_delete_data_task(dag, 'history_ledgers')
delete_old_tx_task = build_delete_data_task(dag, 'history_transactions')
delete_old_asset_task = build_delete_data_task(dag, 'history_assets')

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ledgers_to_bq_task = build_gcs_to_bq_task(dag, ledger_export_task.task_id, 'ledgers', '', partition=True)
send_txs_to_bq_task = build_gcs_to_bq_task(dag, tx_export_task.task_id, 'transactions', '', partition=True)
send_assets_to_bq_task = build_gcs_to_bq_task(dag, asset_export_task.task_id, 'assets', '', partition=False)
 
time_task >> write_ledger_stats >> ledger_export_task >> delete_old_ledger_task >> send_ledgers_to_bq_task
time_task >> write_tx_stats >> tx_export_task >> delete_old_tx_task >> send_txs_to_bq_task
time_task >> write_asset_stats >> asset_export_task  >> delete_old_asset_task >> send_assets_to_bq_task
