'''
The bucket_list_export DAG exports ledger entry changes (accounts, offers, and trustlines) using the history archives' 
bucket list. As a result, it is faster than stellar-core. Bucket list commands require an end ledger that determines when 
to stop exporting. This end ledger  is determined by when the Airflow DAG is run. This DAG should be triggered manually 
when initializing the tables in order to catch up to the current state in the network, but should not be scheduled to run constantly.
'''
import json
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
import datetime
import distutils
import distutils.util
import time

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bucket_list_export',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2021, 10, 15),
    description='This DAG loads a point forward view of state tables. Caution: Does not capture historical changes!',
    schedule_interval='@daily',
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)
use_testnet = bool(distutils.util.strtobool(Variable.get("use_testnet")))

'''
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
'''
time_task = build_time_task(dag, use_next_exec_time=False)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date, 
start and end ledgers so that reconciliation and data validation are easier. The 
record is written to an internal dataset for data eng use only.
'''
write_acc_stats = build_batch_stats(dag, 'accounts')
write_off_stats = build_batch_stats(dag, 'offers')
write_pool_stats = build_batch_stats(dag, 'liquidity_pools')
write_trust_stats = build_batch_stats(dag, 'trustlines')

'''
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the comand are uploaded to GCS. The location is returned through XCOM.
'''
export_acc_task = build_export_task(dag, 'bucket', 'export_accounts', file_names['accounts'], use_testnet=use_testnet, use_gcs=True)
export_off_task = build_export_task(dag, 'bucket', 'export_offers', file_names['offers'], use_testnet=use_testnet, use_gcs=True)
export_pool_task = build_export_task(dag, 'bucket', 'export_pools', file_names['liquidity_pools'], use_testnet=use_testnet, use_gcs=True)
export_trust_task = build_export_task(dag, 'bucket', 'export_trustlines', file_names['trustlines'], use_testnet=use_testnet, use_gcs=True)

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
send_acc_to_bq_task = build_gcs_to_bq_task(dag, export_acc_task.task_id, 'accounts', '', partition=False)
send_off_to_bq_task = build_gcs_to_bq_task(dag, export_off_task.task_id, 'offers', '', partition=False)
send_pool_to_bq_task = build_gcs_to_bq_task(dag, export_pool_task.task_id, 'liquidity_pools', '', partition=False)
send_trust_to_bq_task = build_gcs_to_bq_task(dag, export_trust_task.task_id, 'trustlines', '',  partition=False)

time_task >> write_acc_stats >> export_acc_task >> send_acc_to_bq_task
time_task >> write_off_stats >> export_off_task >> send_off_to_bq_task
time_task >> write_pool_stats >> export_pool_task >> send_pool_to_bq_task
time_task >> write_trust_stats >> export_trust_task >> send_trust_to_bq_task