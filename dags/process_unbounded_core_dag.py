'''
The process_unbounded_core DAG reads information from the unbounded_core_export DAG. The unbounded_core_export exports 
data in batches to a folder as the network progresses. The process_unbounded_core DAG uses file sensors to read these
batches as they are written. Once a batch is read, it is loaded into Google Cloud Storage and loaded into BigQuery.
This DAG is scheduled to run once, and it triggers a new run with the trigger_next every time a batch is processed.
'''

from stellar_etl_airflow.build_file_sensor_task import build_file_sensor_task
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_merge_table_task import build_apply_gcs_changes_to_bq_task

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    'process_unbounded_core',
    default_args=get_default_dag_args(),
    description='This DAG reads data from the unbounded stellar-core instance. The exported accounts, offers, and trustlines are uploaded BigQuery.',
    schedule_interval='@once',
)

account_sensor = build_file_sensor_task(dag, 'accounts')
offer_sensor = build_file_sensor_task(dag, 'offers')
trustline_sensor = build_file_sensor_task(dag, 'trustlines') 

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_accounts_task = build_load_task(dag, 'accounts', 'accounts_file_sensor')
load_offers_task = build_load_task(dag, 'offers', 'offers_file_sensor')
load_trustlines_task = build_load_task(dag, 'trustlines', 'trustlines_file_sensor')

'''
The send tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task appends the entries in the file to the corresponding table in BigQuery. 
'''
apply_account_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'accounts')
apply_offer_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'offers')
apply_trustline_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'trustlines')


'''
This task triggers the next run of this DAG once accounts, offers, and trustlines have been processed.
'''
trigger_next = BashOperator(task_id="trigger_next", 
           bash_command="airflow trigger_dag 'process_unbounded_core'", dag=dag)

account_sensor >> load_accounts_task >> apply_account_changes_task >> trigger_next
offer_sensor >> load_offers_task >> apply_offer_changes_task >> trigger_next
trustline_sensor >> load_trustlines_task >> apply_trustline_changes_task >> trigger_next