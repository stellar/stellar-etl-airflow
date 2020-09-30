from datetime import timedelta
from airflow.models import Variable

def get_default_dag_args():
    owner_name = Variable.get('owner')
    return {
    'owner': owner_name,
    'depends_on_past': False,
    'start_date': "2015-09-30T16:41:54+00:00",
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

def get_default_kubernetes_affinity():
    return {
                'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            'values': [
                                Variable.get('pool_name')
                            ]
                        }]
                    }]
                }
                }
            }
