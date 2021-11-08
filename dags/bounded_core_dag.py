'''
The bounded_core_export DAG exports ledger entry changes (accounts, offers, and trustlines) within a bounded range using stellar-core. 
This DAG should be triggered manually if it is required to export entry changes within a specified time range. For consistent updates, use
the unbounded version.
'''
import json
from time import time

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
import datetime
import time

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bounded_core_export',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2021, 10, 14),
    description='This DAG runs a bounded stellar-core instance, which allows it to export accounts, offers, liquidity pools, and trustlines to BigQuery.',
    schedule_interval='*/15 * * * *',
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

date_task = build_time_task(dag)

file_names = Variable.get('output_file_names', deserialize_json=True)
changes_task = build_export_task(dag, 'bounded-core', 'export_ledger_entry_changes', file_names['changes'])
changes_task.post_execute = lambda **x: time.sleep(60)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date, 
start and end ledgers so that reconciliation and data validation are easier. The 
record is written to an internal dataset for data eng use only.
'''
write_acc_stats = build_batch_stats(dag, 'accounts')
write_off_stats = build_batch_stats(dag, 'offers')
write_pool_stats = build_batch_stats(dag, 'liquidity_pools')
write_trust_stats = build_batch_stats(dag, 'trust_lines')

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_acc_task = build_load_task(dag, 'accounts', 'get_ledger_range_from_times', True)
load_acc_task.pre_execute = lambda **x: time.sleep(30)
load_off_task = build_load_task(dag, 'offers', 'get_ledger_range_from_times', True)
load_off_task.pre_execute = lambda **x: time.sleep(30)
load_pool_task = build_load_task(dag, 'liquidity_pools', 'get_ledger_range_from_times', True)
load_pool_task.pre_execute = lambda **x: time.sleep(30)
load_trust_task = build_load_task(dag, 'trustlines', 'get_ledger_range_from_times', True)
load_trust_task.pre_execute = lambda **x: time.sleep(30)

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
send_acc_to_bq_task = build_gcs_to_bq_task(dag, 'accounts', partition=False)
send_off_to_bq_task = build_gcs_to_bq_task(dag, 'offers', partition=False)
send_pool_to_bq_task = build_gcs_to_bq_task(dag, 'liquidity_pools', partition=False)
send_trust_to_bq_task = build_gcs_to_bq_task(dag, 'trustlines', partition=False)

date_task >> changes_task >> write_acc_stats >> load_acc_task >> send_acc_to_bq_task
date_task >> changes_task >> write_off_stats >> load_off_task >> send_off_to_bq_task
date_task >> changes_task >> write_pool_stats >> load_pool_task >> send_pool_to_bq_task
date_task >> changes_task >> write_trust_stats >> load_trust_task >> send_trust_to_bq_task