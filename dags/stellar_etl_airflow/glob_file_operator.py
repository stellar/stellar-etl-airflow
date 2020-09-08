
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
                 **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id

    def poke(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.filepath)
        self.log.info('Poking for file %s', full_path)

        for path in glob(full_path):
            if os.path.isfile(path):
                return True
        return False