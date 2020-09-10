'''
The unbounded_core_export DAG instantiates a long-running stellar-core instance. This stellar-core instance
exports accounts, offers, and trustlines to a local folder. For each batch of 64 ledgers that is added to the
network, 3 files are created in the folder. There is one folder for each data type. Since this is a long-running 
background task, it should be triggered once manually.
'''
import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'unbounded_core_export',
    default_args=get_default_dag_args(),
    description='This DAG runs an unbounded stellar-core instance, which allows it to export accounts, offers, and trustlines to BigQuery. The core instance will \
        continue running and exporting in the background.',
    schedule_interval=None,
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

date_task = build_time_task(dag, use_next_exec_time=False)

file_names = Variable.get('output_file_names', deserialize_json=True)
changes_task = build_export_task(dag, 'unbounded-core', 'export_ledger_entry_changes', file_names['changes'])

date_task >> changes_task