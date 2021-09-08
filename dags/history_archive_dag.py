'''
The history_archive_export DAG exports ledgers, transactions, operations, and trades from the history archives. 
It is scheduled to export information to BigQuery every 5 minutes. 
'''
import datetime
import json
import time
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

from airflow import DAG
from airflow.models import Variable


dag = DAG(
    'history_archive_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, operations, and trades from the history archive to BigQuery.',
    schedule_interval="0 */3 * * *",
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)

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
write_op_stats = build_batch_stats(dag, 'history_operations')
write_trade_stats = build_batch_stats(dag, 'history_trades')
write_asset_stats = build_batch_stats(dag, 'history_assets')

'''
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the comand are stored in a file. There is one task for each of the data types that 
can be exported from the history archives.

The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task 
starts prematurely and will not load data.
'''
ledger_export_task = build_export_task(dag, 'archive', 'export_ledgers', file_names['ledgers'])
ledger_export_task.post_execute = lambda **x: time.sleep(30)
tx_export_task = build_export_task(dag, 'archive', 'export_transactions', file_names['transactions'])
tx_export_task.post_execute = lambda **x: time.sleep(30)
op_export_task = build_export_task(dag, 'archive', 'export_operations', file_names['operations'])
op_export_task.post_execute = lambda **x: time.sleep(30)
trade_export_task = build_export_task(dag, 'archive', 'export_trades', file_names['trades'])
trade_export_task.post_execute = lambda **x: time.sleep(30)
asset_export_task = build_export_task(dag, 'archive', 'export_assets', file_names['assets'])
asset_export_task.post_execute = lambda **x: time.sleep(30)

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_ledger_task = build_load_task(dag, 'ledgers', 'export_ledgers_task', True)
load_tx_task = build_load_task(dag, 'transactions', 'export_transactions_task', True)
load_op_task = build_load_task(dag, 'operations', 'export_operations_task', True)
load_trade_task = build_load_task(dag, 'trades', 'export_trades_task', True)
load_asset_task = build_load_task(dag, 'assets', 'export_assets_task', True)

'''
The delete partition task checks to see if the given partition/batch id exists in 
Bigquery. If it does, the records are deleted prior to reinserting the batch.
'''
delete_old_ledger_task = build_delete_data_task(dag, 'history_ledgers')
delete_old_tx_task = build_delete_data_task(dag, 'history_transactions')
delete_old_op_task = build_delete_data_task(dag, 'history_operations')
delete_old_trade_task = build_delete_data_task(dag, 'history_trades')
delete_old_asset_task = build_delete_data_task(dag, 'history_assets')

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ledgers_to_bq_task = build_gcs_to_bq_task(dag, 'ledgers', partition=True)
send_txs_to_bq_task = build_gcs_to_bq_task(dag, 'transactions', partition=True)
send_ops_to_bq_task = build_gcs_to_bq_task(dag, 'operations', partition=True)
send_trades_to_bq_task = build_gcs_to_bq_task(dag, 'trades', partition=False)
send_assets_to_bq_task = build_gcs_to_bq_task(dag, 'assets', partition=False)

time_task >> write_ledger_stats >> ledger_export_task >> load_ledger_task >> delete_old_ledger_task >> send_ledgers_to_bq_task
time_task >> write_tx_stats >> tx_export_task >> load_tx_task  >> delete_old_tx_task >>send_txs_to_bq_task
time_task >> write_op_stats >> op_export_task >> load_op_task  >> delete_old_op_task >>send_ops_to_bq_task
time_task >> write_trade_stats >> trade_export_task >> load_trade_task  >> delete_old_trade_task >>send_trades_to_bq_task
time_task >> write_asset_stats >> asset_export_task >> load_asset_task  >> delete_old_asset_task >>send_assets_to_bq_task