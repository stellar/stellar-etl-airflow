from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_date_task import build_date_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bucket_list_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, and operations from the history archive to BigQuery.',
    schedule_interval="*/5 * * * *",
)

file_names = Variable.get('output_file_names', deserialize_json=True)

date_task = build_date_task(dag)

acc_task = build_export_task(dag, 'bucket', 'export_accounts', file_names['accounts'])

off_task = build_export_task(dag, 'bucket', 'export_offers', file_names['offers'])

trust_task = build_export_task(dag, 'bucket', 'export_trustlines', file_names['trustlines'])

date_task >> acc_task
date_task >> off_task
date_task >> trust_task