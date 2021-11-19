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
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

import datetime
import distutils
import distutils.util
import logging
import time

from airflow import DAG
from airflow.models import Variable

logging.basicConfig(format='%(message)s')
logger = logging.getLogger('airflow.task')
logger.setLevel(logging.INFO)

dag = DAG(
    'bounded_core_export',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2021, 10, 14),
    description='This DAG runs a bounded stellar-core instance, which allows it to export accounts, offers, liquidity pools, and trustlines to BigQuery.',
    schedule_interval='*/30 * * * *',
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

use_testnet = bool(distutils.util.strtobool(Variable.get("use_testnet")))
file_names = Variable.get('output_file_names', deserialize_json=True)

date_task = build_time_task(dag)
changes_task = build_export_task(dag, 'bounded-core', 'export_ledger_entry_changes', file_names['changes'], use_testnet=use_testnet, use_gcs=True)

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
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
send_acc_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, 'accounts', '/*-accounts.txt', partition=False)
send_off_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, 'offers', '/*-offers.txt', partition=False)
send_pool_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id,'liquidity_pools', '/*-liquidity_pools.txt', partition=False)
send_trust_to_bq_task = build_gcs_to_bq_task(dag, changes_task.task_id, 'trustlines', '/*-trustlines.txt', partition=False)

date_task >> changes_task >> write_acc_stats >> send_acc_to_bq_task
date_task >> changes_task >> write_off_stats >> send_off_to_bq_task
date_task >> changes_task >> write_pool_stats >> send_pool_to_bq_task
date_task >> changes_task >> write_trust_stats >> send_trust_to_bq_task
