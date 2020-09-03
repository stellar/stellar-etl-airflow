import os
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable

def build_file_sensor_task(dag, data_type):
    file_names = Variable.get('output_file_names', deserialize_json=True)
    folder_name = file_names['changes']
    base_filename = file_names[data_type]
    return FileSensor(
        task_id=f'{data_type}_file_sensor',
        filepath= os.path.join(folder_name, base_filename),
        poke_interval=300,
        mode="reschedule",
        dag=dag
    )