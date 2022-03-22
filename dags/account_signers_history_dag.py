'''
This DAG export claimable balances from start to end date. It should be removed once it's caught up.
'''
import ast
import datetime
import json
import logging

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import init_sentry, get_account_signers_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow import macros

from airflow import DAG
from airflow.models import Variable

init_sentry()

logging.basicConfig(format='%(message)s')
logger = logging.getLogger('airflow.task')
logger.setLevel(logging.INFO)

dag = DAG(
    'account_signers_history',
    default_args=get_account_signers_dag_args(),
    start_date=datetime.datetime(2021, 10, 1, 4),
    end_date=datetime.datetime(2022, 3, 11, 19, 0),
    description='This DAG loads account_signers from archives to BigQuery tables.',
    schedule_interval='0 */4 * * *',
    params={
        'alias': 'state',
    },
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
    user_defined_macros={
        'subtract_data_interval': macros.subtract_data_interval,
        'batch_run_date_as_datetime_string': macros.batch_run_date_as_datetime_string,
    },
)

use_testnet = ast.literal_eval(Variable.get("use_testnet"))
file_names = Variable.get('output_file_names', deserialize_json=True)
table_names = Variable.get('table_ids', deserialize_json=True)
signers_table = table_names['signers']

date_task = build_time_task(dag)
export_sign_task = build_export_task(dag, 'bounded-core', 'export_ledger_entry_changes', file_names['signers'], use_testnet=use_testnet, use_gcs=True, resource_cfg='state')

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date,
start and end ledgers so that reconciliation and data validation are easier. The
record is written to an internal dataset for data eng use only.
'''
write_sign_stats = build_batch_stats(dag, signers_table)

'''
The delete partition task checks to see if the given partition/batch id exists in
Bigquery. If it does, the records are deleted prior to reinserting the batch.
'''
delete_sign_task = build_delete_data_task(dag, signers_table)

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery.
Entries are updated, deleted, or inserted as needed.
'''
send_sign_to_bq_task = build_gcs_to_bq_task(dag, export_sign_task.task_id, signers_table, '/*-signers.txt', partition=False)

date_task >> export_sign_task >> write_sign_stats >> delete_sign_task >> send_sign_to_bq_task
