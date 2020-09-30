'''
This file contains functions for creating Airflow tasks to sense when new files are available.
'''
import os
from stellar_etl_airflow.glob_file_operator import GlobFileSensor
from airflow.models import Variable

def build_file_sensor_task(dag, data_type, include_folder_in_path=False):
    file_names = Variable.get('output_file_names', deserialize_json=True)
    folder_name = file_names['changes']
    filename = f'*{file_names[data_type]}' #include wildcard, as ledger ranges vary
    filepath = os.path.join(Variable.get('output_path'), folder_name, filename) if include_folder_in_path else os.path.join(Variable.get('output_path'), filename) 
    return GlobFileSensor(
        task_id=f'{data_type}_file_sensor',
        filepath=filepath,
        base_path=Variable.get('output_path'),
        poke_interval=60,
        dag=dag
    )