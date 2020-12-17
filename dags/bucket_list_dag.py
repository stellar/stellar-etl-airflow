'''
The bucket_list_export DAG exports ledger entry changes (accounts, offers, and trustlines) using the history archives' 
bucket list. As a result, it is faster than stellar-core. Bucket list commands require an end ledger that determines when 
to stop exporting. This end ledger  is determined by when the Airflow DAG is run. This DAG should be triggered manually 
when initializing the tables in order to catch up to the current state in the network, but should not be scheduled to run constantly.
'''
import json
from stellar_etl_airflow.build_export_task import build_export_task
from stellar_etl_airflow.build_time_task import build_time_task
from stellar_etl_airflow.build_load_task import build_load_task
from stellar_etl_airflow.default import get_default_dag_args
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import build_apply_gcs_changes_to_bq_task

from airflow import DAG
from airflow.models import Variable

dag = DAG(
    'bucket_list_export',
    default_args=get_default_dag_args(),
    description='This DAG exports ledgers, transactions, and operations from the history archive to BigQuery.',
    schedule_interval=None,
    user_defined_filters={'fromjson': lambda s: json.loads(s)},
)

file_names = Variable.get('output_file_names', deserialize_json=True)

time_task = build_time_task(dag, use_next_exec_time=False)

export_acc_task = build_export_task(dag, 'bucket', 'export_accounts', file_names['accounts'])
export_off_task = build_export_task(dag, 'bucket', 'export_offers', file_names['offers'])
export_trust_task = build_export_task(dag, 'bucket', 'export_trustlines', file_names['trustlines'])

'''
The load tasks receive the location of the exported file through Airflow's XCOM system.
Then, the task loads the file into Google Cloud Storage. Finally, the file is deleted
from local storage.
'''
load_acc_task = build_load_task(dag, 'accounts', 'export_accounts_task')
load_off_task = build_load_task(dag, 'offers', 'export_offers_task')
load_trust_task = build_load_task(dag, 'trustlines', 'export_trustlines_task')

'''
The apply tasks receive the location of the file in Google Cloud storage through Airflow's XCOM system.
Then, the task merges the entries in the file with the entries in the corresponding table in BigQuery. 
Entries are updated, deleted, or inserted as needed.
'''
apply_account_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'accounts')
apply_offer_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'offers')
apply_trustline_changes_task = build_apply_gcs_changes_to_bq_task(dag, 'trustlines')

time_task >> export_acc_task >> load_acc_task >> apply_account_changes_task
time_task >> export_off_task >> load_off_task >> apply_offer_changes_task
time_task >> export_trust_task >> load_trust_task >> apply_trustline_changes_task