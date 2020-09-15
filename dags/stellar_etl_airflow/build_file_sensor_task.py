'''
This file contains functions for creating Airflow tasks to sense when new files are available.
'''
import os
from stellar_etl_airflow.glob_file_operator import GlobFileSensor
from airflow.models import Variable

def build_file_sensor_task(dag, data_type):
    file_names = Variable.get('output_file_names', deserialize_json=True)
    folder_name = file_names['changes']
    filename = f'*{file_names[data_type]}' #include wildcard, as ledger ranges vary
    return GlobFileSensor(
        task_id=f'{data_type}_file_sensor',
        filepath=os.path.join(Variable.get('output_path'), folder_name, filename),
        base_path=Variable.get('output_path'),
        poke_interval=60,
        dag=dag
    )