from datetime import timedelta
from airflow.models import Variable

#Note: on local versions of Airflow the path may be airflow.kubernetes
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
  
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
                                'etl-pool', #TODO: replace with variable
                            ]
                        }]
                    }]
                }
                }
            }

def get_default_kubernetes_volumes():
    volume_config = {
            'hostPath': {
                'path': Variable.get('output_path'),
                'type': 'Directory'
            }
        }
    volume = Volume(name='etl-volume', configs=volume_config)
    return [volume]         

def get_default_kubernetes_volume_mounts(): 
    mount = VolumeMount('etl-volume', #TODO: make configurable
    mount_path='/etl/exported_data/',
    sub_path=None,
    read_only=False)
    return [mount]   