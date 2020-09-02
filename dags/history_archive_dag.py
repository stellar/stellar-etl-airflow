'''
The history_archive_export DAG exports ledgers, transactions, operations, and trades from the history archives. 
It is scheduled to export information to BigQuery every 5 minutes. 
'''

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'history_archive_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, operations, and trades from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
)

file_names = Variable.get('output_file_names', deserialize_json=True)

date_task = build_time_task(dag)

ledger_export_task = build_export_task(dag, 'archive', 'export_ledgers', file_names['ledgers'])
tx_export_task = build_export_task(dag, 'archive', 'export_transactions', file_names['transactions'])
op_export_task = build_export_task(dag, 'archive', 'export_operations', file_names['operations'])
trade_export_task = build_export_task(dag, 'archive', 'export_trades', file_names['trades'])

load_ledger_task = build_load_task(dag, 'ledgers')
load_tx_task = build_load_task(dag, 'transactions')
load_op_task = build_load_task(dag, 'operations')
load_trade_task = build_load_task(dag, 'trades')

send_ledgers_to_bq_task = build_gcs_to_bq_task(dag, 'ledgers')
send_txs_to_bq_task = build_gcs_to_bq_task(dag, 'transactions')
send_ops_to_bq_task = build_gcs_to_bq_task(dag, 'operations')
send_trades_to_bq_task = build_gcs_to_bq_task(dag, 'trades')

date_task >> ledger_export_task >> load_ledger_task >> send_ledgers_to_bq_task
date_task >> tx_export_task >> load_tx_task  >> send_txs_to_bq_task
date_task >> op_export_task >> load_op_task  >> send_ops_to_bq_task
date_task >> trade_export_task >> load_trade_task  >> send_trades_to_bq_task