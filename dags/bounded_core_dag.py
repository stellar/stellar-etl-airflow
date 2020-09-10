'''
The bounded_core_export DAG exports ledger entry changes (accounts, offers, and trustlines) within a bounded range using stellar-core. 
This DAG should be triggered manually if it is required to export entry changes within a specified time range. For consistent updates, use
the unbounded version.
'''
import json

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bounded_core_export',
    default_args=get_default_dag_args(),
    description='This DAG runs a bounded stellar-core instance, which allows it to export accounts, offers, and trustlines to BigQuery.',
    schedule_interval=None,
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

date_task = build_time_task(dag, use_next_exec_time=False)

file_names = Variable.get('output_file_names', deserialize_json=True)
changes_task = build_export_task(dag, 'bounded-core', 'export_ledger_entry_changes', file_names['changes'])

date_task >> changes_task