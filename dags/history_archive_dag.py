'''
The history_archive_export DAG exports ledgers, transactions, operations, and trades from the history archives. 
It is scheduled to export information to BigQuery every 5 minutes. 
'''

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_load_task import build_load_task

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'history_archive_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, operations, and trades from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
)

file_names = Variable.get('output_file_names', deserialize_json=True)
table_ids = Variable.get('table_ids', deserialize_json=True)

date_task = build_time_task(dag)

ledger_export_task = build_export_task(dag, 'archive', 'export_ledgers', file_names['ledgers'])

tx_export_task = build_export_task(dag, 'archive', 'export_transactions', file_names['transactions'])

op_export_task = build_export_task(dag, 'archive', 'export_operations', file_names['operations'])

date_task >> ledger_export_task
date_task >> tx_export_task
date_task >> op_export_task

load_ledger_task = build_load_task(dag, 'ledgers', file_names['ledgers'])
ledger_export_task >> load_ledger_task

load_tx_task = build_load_task(dag, 'transactions', file_names['transactions'])
tx_export_task >> load_tx_task

load_op_task = build_load_task(dag, 'operations', file_names['operations'])
op_export_task >> load_op_task