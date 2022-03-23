from datetime import timedelta, datetime
from airflow.models import Variable
import sentry_sdk
from sentry_sdk import set_tag

def init_sentry():
    sentry_sdk.init(
        dsn=Variable.get('sentry_dsn'),
        environment=Variable.get('sentry_environment'),
    )
    set_tag("image_version", Variable.get('image_name'))

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
