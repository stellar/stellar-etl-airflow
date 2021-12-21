'''
This DAG export claimable balances state from archives. Run this once.
'''
import ast
import datetime
import json
import logging

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_claimable_balances_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

from airflow import DAG
from airflow.models import Variable

logging.basicConfig(format='%(message)s')
logger = logging.getLogger('airflow.task')
logger.setLevel(logging.ERROR)

dag = DAG(
    'claimable_balances_bucket_list',
    default_args=get_claimable_balances_dag_args(),
    start_date=datetime.datetime(2021, 10, 1),
    end_date=datetime.datetime(2021, 10, 1),
    description='This DAG loads claimable_balances from archives to BigQuery tables.',
    schedule_interval='@daily',
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

use_testnet = ast.literal_eval(Variable.get('use_testnet'))
file_names = Variable.get('output_file_names', deserialize_json=True)

date_task = build_time_task(dag)
export_bal_task = build_export_task(dag, 'bucket', 'export_claimable_balances', file_names['claimable_balances'], use_testnet=use_testnet, use_gcs=True)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date,
start and end ledgers so that reconciliation and data validation are easier. The
record is written to an internal dataset for data eng use only.
'''
write_bal_stats = build_batch_stats(dag, 'claimable_balances')

'''
The delete partition task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.
'''
delete_bal_task = build_delete_data_task(dag, 'claimable_balances')

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery.
Entries are updated, deleted, or inserted as needed.
'''
send_bal_to_bq_task = build_gcs_to_bq_task(dag, export_bal_task.task_id, 'claimable_balances', '', partition=False)

date_task >> export_bal_task >> write_bal_stats >> delete_bal_task >> send_bal_to_bq_task
