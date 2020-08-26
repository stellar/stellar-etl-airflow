from datetime import timedelta
from airflow.models import Variable

def get_default_dag_args():
    owner_name = Variable.get('owner')
    return {
    'owner': owner_name,
    'depends_on_past': False,
    'start_date': "2015-09-30T16:46:54+00:00",
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}