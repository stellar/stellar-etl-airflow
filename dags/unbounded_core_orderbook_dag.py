'''
The unbounded_core_orderbook_export DAG instantiates a long-running stellar-core instance. This stellar-core instance
exports normalized orderbooks to a local folder. For each batch of 64 ledgers that is added to the
network, 4 files are created in the folder. The files are dimAccounts.txt, dimOffers.txt, dimMarkets.txt, and factEvents.txt. 
Since this is a long-running background task, it should be triggered once manually.
'''
import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_orderbook_dag_args

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'unbounded_core_orderbook_export',
    default_args=get_orderbook_dag_args(),
    description='This DAG runs an unbounded stellar-core instance, which allows it to export orderbooks to BigQuery. The core instance will \
        continue running and exporting in the background.',
    schedule_interval="@once",
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

date_task = build_time_task(dag, use_next_exec_time=False)

file_names = Variable.get('output_file_names', deserialize_json=True)
orderbook_task = build_export_task(dag, 'unbounded-core', 'export_orderbooks', file_names['orderbooks'])
orderbook_task.retries = 0

date_task >> orderbook_task