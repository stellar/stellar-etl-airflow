'''
The unbounded_core_export DAG instantiates a long-running stellar-core instance. This stellar-core instance
exports accounts, offers, and trustlines to a local folder. For each batch of 64 ledgers that is added to the
network, 3 files are created in the folder. There is one folder for each data type. Since this is a long-running 
background task, it should be triggered once manually.
'''

from stellar_etl_airflow.build_file_sensor_task import build_file_sensor_task
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    'process_unbounded_core',
    default_args=get_default_dag_args(),
    description='This DAG runs an unbounded stellar-core instance, which allows it to export accounts, offers, and trustlines to BigQuery. The core instance will \
        continue running and exporting in the background.',
    schedule_interval=None,
)

account_sensor = build_file_sensor_task(dag, 'accounts')
offer_sensor = build_file_sensor_task(dag, 'offers')
trustline_sensor = build_file_sensor_task(dag, 'trustlines') 

load_accounts_task = build_load_task(dag, 'accounts')
load_offers_task = build_load_task(dag, 'offers')
load_trustlines_task = build_load_task(dag, 'trustlines')

trigger_next = BashOperator(task_id="trigger_next", 
           bash_command="airflow trigger_dag 'process_unbounded_core'", dag=dag)

account_sensor >> load_accounts_task >> trigger_next
offer_sensor >> load_offers_task >> trigger_next
trustline_sensor >> load_trustlines_task >> trigger_next