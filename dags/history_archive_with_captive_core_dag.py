'''
The history_archive_export DAG exports operations and trades from the history archives. 
It is scheduled to export information to BigQuery at regular intervals.
'''
import ast
import datetime
import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import init_sentry, get_default_dag_args
from stellar_etl_airflow.build_batch_stats import build_batch_stats
from stellar_etl_airflow.build_delete_data_task import build_delete_data_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_bq_insert_job_task import build_bq_insert_job
from stellar_etl_airflow import macros

from airflow import DAG
from airflow.models import Variable

init_sentry()

dag = DAG(
    'history_archive_with_captive_core',
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2022, 3, 11, 18, 30),
    description='This DAG exports trades and operations from the history archive using CaptiveCore. This supports parsing sponsorship and AMMs.',
    schedule_interval='*/30 * * * *',
    params={
        'alias': 'cc',
    },
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
    user_defined_macros={
        'subtract_data_interval': macros.subtract_data_interval,
        'batch_run_date_as_datetime_string': macros.batch_run_date_as_datetime_string,
    },
)

file_names = Variable.get('output_file_names', deserialize_json=True)
table_names = Variable.get('table_ids', deserialize_json=True)
internal_project = Variable.get('bq_project')
internal_dataset = Variable.get('bq_dataset')
public_project = Variable.get('public_project')
public_dataset = Variable.get('public_dataset')
use_testnet = ast.literal_eval(Variable.get("use_testnet"))

'''
The time task reads in the execution time of the current run, as well as the next
execution time. It converts these two times into ledger ranges.
'''
time_task = build_time_task(dag, use_testnet=use_testnet)

'''
The write batch stats task will take a snapshot of the DAG run_id, execution date, 
start and end ledgers so that reconciliation and data validation are easier. The 
record is written to an internal dataset for data eng use only.
'''
write_op_stats = build_batch_stats(dag, table_names['operations'])
write_trade_stats = build_batch_stats(dag, table_names['trades'])

'''
The export tasks call export commands on the Stellar ETL using the ledger range from the time task.
The results of the command are stored in a file. There is one task for each of the data types that
can be exported from the history archives.

The DAG sleeps for 30 seconds after the export_task writes to the file to give the poststart.sh
script time to copy the file over to the correct directory. If there is no sleep, the load task 
starts prematurely and will not load data.
'''
op_export_task = build_export_task(dag, 'archive', 'export_operations', file_names['operations'], use_testnet=use_testnet, use_gcs=True, resource_cfg='cc')
trade_export_task = build_export_task(dag, 'archive', 'export_trades', file_names['trades'], use_testnet=use_testnet, use_gcs=True, resource_cfg='cc')

'''
The delete partition task checks to see if the given partition/batch id exists in 
Bigquery. If it does, the records are deleted prior to reinserting the batch.
'''
delete_old_op_task = build_delete_data_task(dag, internal_project, internal_dataset, table_names['operations'])
delete_old_op_pub_task = build_delete_data_task(dag, public_project, public_dataset, table_names['operations'])
delete_old_trade_task = build_delete_data_task(dag, internal_project, internal_dataset, table_names['trades'])
delete_old_trade_pub_task = build_delete_data_task(dag, public_project, public_dataset, table_names['trades'])
delete_enrich_op_task = build_delete_data_task(dag, internal_project, internal_dataset, 'enriched_history_operations')
delete_enrich_op_pub_task = build_delete_data_task(dag, public_project, public_dataset, 'enriched_history_operations')
delete_enrich_ma_op_task = build_delete_data_task(dag, internal_project, internal_dataset, 'enriched_meaningful_history_operations')

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ops_to_bq_task = build_gcs_to_bq_task(dag, op_export_task.task_id, internal_project, internal_dataset, table_names['operations'], '', partition=True, cluster=False)
send_trades_to_bq_task = build_gcs_to_bq_task(dag, trade_export_task.task_id, internal_project, internal_dataset, table_names['trades'], '', partition=False, cluster=False)

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the unique entries in the file into the corresponding table in BigQuery. 
'''
send_ops_to_pub_task = build_gcs_to_bq_task(dag, op_export_task.task_id, public_project, public_dataset, table_names['operations'], '', partition=True, cluster=True)
send_trades_to_pub_task = build_gcs_to_bq_task(dag, trade_export_task.task_id, public_project, public_dataset, table_names['trades'], '', partition=True, cluster=True)

'''
Batch loading of derived table, `enriched_history_operations` which denormalizes ledgers, transactions and operations data.
Must wait on history_archive_without_captive_core_dag to finish before beginning the job.
The internal dataset also creates a filtered table, `enriched_meaningful_history_operations` which filters down to only relevant asset ops.
'''
wait_on_dag = build_cross_deps(dag, "wait_on_ledgers_txs", "history_archive_without_captive_core")
insert_enriched_hist_task = build_bq_insert_job(dag, internal_project, internal_dataset, "enriched_history_operations", partition=True)
insert_enriched_hist_pub_task = build_bq_insert_job(dag, public_project, public_dataset, "enriched_history_operations", partition=True)
insert_enriched_ma_hist_task = build_bq_insert_job(dag, internal_project, internal_dataset, "enriched_meaningful_history_operations", partition=True)

time_task >> write_op_stats >> op_export_task >> delete_old_op_task >> send_ops_to_bq_task >> wait_on_dag >> insert_enriched_hist_task >> delete_enrich_ma_op_task >> insert_enriched_ma_hist_task
op_export_task >> delete_old_op_pub_task >> send_ops_to_pub_task >> wait_on_dag >> insert_enriched_hist_pub_task
time_task >> write_trade_stats >> trade_export_task  >> delete_old_trade_task >> send_trades_to_bq_task
trade_export_task >> delete_old_trade_pub_task >> send_trades_to_pub_task