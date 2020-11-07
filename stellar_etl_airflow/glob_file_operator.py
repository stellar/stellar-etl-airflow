
import os
from glob import glob

from airflow.contrib.hooks.fs_hook import FSHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class GlobFileSensor(BaseSensorOperator):
    '''
    This sensor waits until the provided file exists.
    '''
    template_fields = ('filepath',)
    ui_color = '#91818a'

    @apply_defaults
    def __init__(self, *,
                 filepath,
                 fs_conn_id='fs_default',
                 base_path='/',
                 **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id
        self.base_path = base_path

    def poke(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.filepath)
        self.log.info('Poking for file %s', full_path)

        valid_files = []
        for path in glob(full_path):
            if os.path.isfile(path):
                valid_files.append(path)
        if valid_files:
            valid_files.sort()
            self.log.info(f'The full list of valid files is: ({", ".join(valid_files)})')
            relative_path = os.path.relpath(valid_files[0], start=self.base_path)
            self.log.info(f'Relative path of the earliest file is: {relative_path}')
            context['ti'].xcom_push(
                key='return_value',
                value=relative_path,
                execution_date=context['execution_date'])
            return True
        else:
            return False
        