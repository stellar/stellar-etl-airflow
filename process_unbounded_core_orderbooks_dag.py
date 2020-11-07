'''
The process_unbounded_core_changes DAG reads information from the unbounded_core_changes_export DAG. The unbounded_core_changes_export exports 
data in batches to a folder as the network progresses. The process_unbounded_core_changes DAG uses file sensors to read these
batches as they are written. Once a batch is read, it is loaded into Google Cloud Storage and loaded into BigQuery.
This DAG is scheduled to run once, and it triggers a new run with the trigger_next every time a batch is processed.
'''
from stellar_etl_airflow.build_file_sensor_task import build_file_sensor_task
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import build_apply_gcs_changes_to_bq_task
from stellar_etl_airflow.build_gcs_to_bq_task import build_gcs_to_bq_task

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    'process_unbounded_core_orderbooks',
    default_args=get_default_dag_args(),
    description='This DAG reads data from the unbounded stellar-core instance. The exported normalized orderbook files are uploaded BigQuery.',
    schedule_interval='@once',
)

dim_account_sensor = build_file_sensor_task(dag, 'dimAccounts', is_orderbook=True)
dim_offer_sensor = build_file_sensor_task(dag, 'dimOffers', is_orderbook=True)
dim_markets_sensor = build_file_sensor_task(dag, 'dimMarkets', is_orderbook=True) 
fact_events_sensor = build_file_sensor_task(dag, 'factEvents', is_orderbook=True) 

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_dim_accounts_task = build_load_task(dag, 'dimAccounts', 'dimAccounts_file_sensor')
load_dim_offers_task = build_load_task(dag, 'dimOffers', 'dimOffers_file_sensor')
load_dim_markets_task = build_load_task(dag, 'dimMarkets', 'dimMarkets_file_sensor')
load_fact_events_task = build_load_task(dag, 'factEvents', 'factEvents_file_sensor')

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
apply_dim_account_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'dimAccounts')
apply_dim_offer_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'dimOffers')
apply_dim_markets_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'dimMarkets')
send_fact_events_to_bq_task = build_gcs_to_bq_task(dag, 'factEvents')


'''
This task triggers the next run of this DAG once accounts, offers, and trustlines have been processed.
'''
trigger_next = BashOperator(task_id="trigger_next", 
           bash_command="airflow trigger_dag 'process_unbounded_core'", dag=dag)

dim_account_sensor >> load_dim_accounts_task >> apply_dim_account_changes_task >> trigger_next
dim_offer_sensor >> load_dim_offers_task >> apply_dim_offer_changes_task >> trigger_next
dim_markets_sensor >> load_dim_markets_task >> apply_dim_markets_changes_task >> trigger_next
fact_events_sensor >> load_fact_events_task >> send_fact_events_to_bq_task >> trigger_next