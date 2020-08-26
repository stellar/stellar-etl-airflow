import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_date_task import build_date_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG

dag = DAG(
    'bucket_list_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, and operations from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
)

date_task = build_date_task(dag)

acc_task = build_export_task(dag, 'bucket', 'export_accounts', 'accounts.txt')

off_task = build_export_task(dag, 'bucket', 'export_offers', 'offers.txt')

trust_task = build_export_task(dag, 'bucket', 'export_trustlines', 'trustlines.txt')

date_task >> acc_task
date_task >> off_task
date_task >> trust_task