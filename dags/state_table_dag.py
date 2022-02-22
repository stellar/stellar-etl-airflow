'''
The state_table_export DAG exports ledger entry changes (accounts, offers, and trustlines) within a bounded range using stellar-core. 
This DAG should be triggered manually if it is required to export entry changes within a specified time range. 
'''
import ast
import datetime
import json
import logging

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

from airflow import DAG
from airflow.models import Variable

init_sentry()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger('airflow.task')
logger.setLevel(logging.INFO)

dag = DAG(
    'state_table_export',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2021, 11, 29, 22, 30),
    description='This DAG runs a bounded stellar-core instance, which allows it to export accounts, offers, liquidity pools, and trustlines to BigQuery.',
    schedule_interval='*/30 * * * *',
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)
table_names = Variable.get('table_ids', deserialize_json=True)
use_testnet = ast.literal_eval(Variable.get("use_testnet"))

date_task = build_time_task(dag, use_testnet=use_testnet)
changes_task = build_export_task(dag, 'bounded-core', 'export_ledger_entry_changes', file_names['changes'], use_testnet=use_testnet, use_gcs=True)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date, 
start and end ledgers so that reconciliation and data validation are easier. The 
record is written to an internal dataset for data eng use only.
'''
write_acc_stats = build_batch_stats(dag, table_names['accounts'])
write_bal_stats = build_batch_stats(dag, table_names['claimable_balances'])
write_off_stats = build_batch_stats(dag, table_names['offers'])
write_pool_stats = build_batch_stats(dag, table_names['liquidity_pools'])
write_sign_stats = build_batch_stats(dag, table_names['signers'])
write_trust_stats = build_batch_stats(dag, table_names['trustlines'])

'''
The delete partition task checks to see if the given partition/batch id exists in 
Bigquery. If it does, the records are deleted prior to reinserting the batch.
'''
delete_acc_task = build_delete_data_task(dag, table_names['accounts'])
delete_bal_task = build_delete_data_task(dag, table_names['claimable_balances'])
delete_off_task = build_delete_data_task(dag, table_names['offers'])
delete_pool_task = build_delete_data_task(dag, table_names['liquidity_pools'])
delete_sign_task = build_delete_data_task(dag, table_names['signers'])
delete_trust_task = build_delete_data_task(dag, table_names['trustlines'])

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
send_acc_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, table_names['accounts'], '/*-accounts.txt', partition=False)
send_bal_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, table_names['claimable_balances'], '/*-claimable_balances.txt', partition=False)
send_off_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, table_names['offers'], '/*-offers.txt', partition=False)
send_pool_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id,table_names['liquidity_pools'], '/*-liquidity_pools.txt', partition=False)
send_sign_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, table_names['signers'], '/*-signers.txt', partition=False)
send_trust_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, table_names['trustlines'], '/*-trustlines.txt', partition=False)

date_task >> changes_task >> write_acc_stats >> delete_acc_task >> send_acc_to_bq_task
date_task >> changes_task >> write_bal_stats >> delete_bal_task >> send_bal_to_bq_task
date_task >> changes_task >> write_off_stats >> delete_off_task >> send_off_to_bq_task
date_task >> changes_task >> write_pool_stats >> delete_pool_task >> send_pool_to_bq_task
date_task >> changes_task >> write_sign_stats >> delete_sign_task >> send_sign_to_bq_task
date_task >> changes_task >> write_trust_stats >> delete_trust_task >> send_trust_to_bq_task
