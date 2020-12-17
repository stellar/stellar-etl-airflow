from datetime import timedelta
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
    base['start_date'] = "2015-09-30T16:41:54+00:00"
    return base

def get_orderbook_dag_args():
    base = get_base_dag_args()
    base['start_date'] = "2015-09-30T17:26:17+00:00"
    return base