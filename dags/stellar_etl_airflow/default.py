from datetime import timedelta, datetime
from airflow.models import Variable

def get_base_dag_args():
    return {
        'owner': Variable.get('owner'),
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
    }

def get_default_dag_args():
    base = get_base_dag_args()
    base['start_date'] = datetime(2021, 8, 31)
    return base

def get_orderbook_dag_args():
    base = get_base_dag_args()
    base['start_date'] = datetime(2021, 8, 31)
    return base

def get_account_signers_dag_args():
    base = get_base_dag_args()
    base['start_date'] = datetime(2021, 10, 1)
    return base