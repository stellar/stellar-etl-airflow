import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_date_task import build_date_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG

dag = DAG(
    'history_archive_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, and operations from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
)

date_task = build_date_task(dag)

ledger_task = build_export_task(dag, 'archive', 'export_ledgers', 'ledgers.txt')

tx_task = build_export_task(dag, 'archive', 'export_transactions', 'transactions.txt')

op_task = build_export_task(dag, 'archive', 'export_operations', 'operations.txt')

date_task >> ledger_task
date_task >> tx_task
date_task >> op_task