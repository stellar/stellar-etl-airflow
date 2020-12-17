'''
The history_archive_export DAG exports ledgers, transactions, operations, and trades from the history archives. 
It is scheduled to export information to BigQuery every 5 minutes. 
'''
import json
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import build_apply_gcs_changes_to_bq_task

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'history_archive_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, operations, and trades from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)

'''
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
'''
time_task = build_time_task(dag)

'''
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the comand are stored in a file. There is one task for each of the data types that 
can be exported from the history archives.
'''
ledger_export_task = build_export_task(dag, 'archive', 'export_ledgers', file_names['ledgers'])
tx_export_task = build_export_task(dag, 'archive', 'export_transactions', file_names['transactions'])
op_export_task = build_export_task(dag, 'archive', 'export_operations', file_names['operations'])
trade_export_task = build_export_task(dag, 'archive', 'export_trades', file_names['trades'])

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_ledger_task = build_load_task(dag, 'ledgers', 'export_ledgers_task')
load_tx_task = build_load_task(dag, 'transactions', 'export_transactions_task')
load_op_task = build_load_task(dag, 'operations', 'export_operations_task')
load_trade_task = build_load_task(dag, 'trades', 'export_trades_task')

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ledgers_to_bq_task = build_apply_gcs_changes_to_bq_task(dag, 'ledgers')
send_txs_to_bq_task = build_apply_gcs_changes_to_bq_task(dag, 'transactions')
send_ops_to_bq_task = build_apply_gcs_changes_to_bq_task(dag, 'operations')
send_trades_to_bq_task = build_apply_gcs_changes_to_bq_task(dag, 'trades')

time_task >> ledger_export_task >> load_ledger_task >> send_ledgers_to_bq_task
time_task >> tx_export_task >> load_tx_task  >> send_txs_to_bq_task
time_task >> op_export_task >> load_op_task  >> send_ops_to_bq_task
time_task >> trade_export_task >> load_trade_task  >> send_trades_to_bq_task