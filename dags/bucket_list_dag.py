'''
The bucket_list_export DAG exports ledger entry changes (accounts, offers, and trustlines) using the history archives' 
bucket list. As a result, it is faster than stellar-core. Bucket list commands require an end ledger that determines when 
to stop exporting. This end ledger  is determined by when the Airflow DAG is run. This DAG should be triggered manually 
when initializing the tables in order to catch up to the current state in the network, but should not be scheduled to run constantly.
'''

from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_date_task import build_date_task
from stellar_etl_airflow.default import get_default_dag_args

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bucket_list_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, and operations from the history archive to BigQuery.',
    schedule_interval=None,
)

file_names = Variable.get('output_file_names', deserialize_json=True)

date_task = build_date_task(dag)

acc_task = build_export_task(dag, 'bucket', 'export_accounts', file_names['accounts'])

off_task = build_export_task(dag, 'bucket', 'export_offers', file_names['offers'])

trust_task = build_export_task(dag, 'bucket', 'export_trustlines', file_names['trustlines'])

date_task >> acc_task
date_task >> off_task
date_task >> trust_task