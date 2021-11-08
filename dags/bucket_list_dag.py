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
import time
import datetime

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bucket_list_export',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2021, 10, 15),
    end_date=datetime.datetime(2021, 10, 15),
    description='This DAG loads a point forward view of state tables. Caution: Does not capture historical changes!',
    schedule_interval="@daily",
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)

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
The results of the comand are stored in a file. There is one task for each of the data types that 
can be exported from the history archives.

The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task 
starts prematurely and will not load data.
'''
export_acc_task = build_export_task(dag, 'bucket', 'export_accounts', file_names['accounts'])
export_acc_task.post_execute = lambda **x: time.sleep(60)
export_off_task = build_export_task(dag, 'bucket', 'export_offers', file_names['offers'])
export_off_task.post_execute = lambda **x: time.sleep(60)
export_pool_task = build_export_task(dag, 'bucket', 'export_pools', file_names['liquidity_pools'])
export_pool_task.post_execute = lambda **x: time.sleep(60)
export_trust_task = build_export_task(dag, 'bucket', 'export_trustlines', file_names['trustlines'])
export_trust_task.post_execute = lambda **x: time.sleep(60)

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_acc_task = build_load_task(dag, 'accounts', 'export_accounts_task', True)
load_off_task = build_load_task(dag, 'offers', 'export_offers_task', True)
load_pool_task = build_load_task(dag, 'liqudity_pools', 'export_pools_task', True)
load_trust_task = build_load_task(dag, 'trustlines', 'export_trustlines_task', True)

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
send_acc_to_bq_task = build_gcs_to_bq_task(dag, 'accounts', partition=False)
send_off_to_bq_task = build_gcs_to_bq_task(dag, 'offers', partition=False)
send_pool_to_bq_task = build_gcs_to_bq_task(dag, 'liquidity_pools', partition=False)
send_trust_to_bq_task = build_gcs_to_bq_task(dag, 'trustlines', partition=False)

time_task >> write_acc_stats >> export_acc_task >> load_acc_task >> send_acc_to_bq_task
time_task >> write_off_stats >> export_off_task >> load_off_task >> send_off_to_bq_task
time_task >> write_pool_stats >> export_pool_task >> load_pool_task >> send_pool_to_bq_task
time_task >> write_trust_stats >> export_trust_task >> load_trust_task >> send_trust_to_bq_task