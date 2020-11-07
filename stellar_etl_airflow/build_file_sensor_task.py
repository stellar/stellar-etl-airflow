'''
This file contains functions for creating Airflow tasks to sense when new files are available.
'''
import os
from stellar_etl_airflow.glob_file_operator import GlobFileSensor
from airflow.models import Variable

def build_file_sensor_task(dag, data_type, is_change=False, is_orderbook=False):
    '''
    Creates a task that senses for local files.
    Data types should be: 'accounts', 'ledgers', 'offers', 'operations', 'trades', 'transactions', 'trustlines',
    'dimAccounts', 'dimOffers', 'dimMarkets', or 'factEvents'.
    
    Parameters:
        dag - the parent dag
        data_type - type of the data being uploaded; should be string
        is_change - True if the file sensor should look in the changes folder
        is_orderbook - True if the file sensor should look in the orderbook folder
    Returns:
        the newly created task
    '''
    file_names = Variable.get('output_file_names', deserialize_json=True)
    change_folder_name = file_names['changes']
    orderbook_folder_name = file_names['orderbooks']
    filename = f'*{file_names[data_type]}' #include wildcard, as ledger ranges vary
    filepath = filename
    if is_change:
        filepath = os.path.join(change_folder_name, filename)
    elif is_orderbook:
        filepath = os.path.join(orderbook_folder_name, filename)
    filepath = os.path.join(Variable.get('output_path'), filepath)
    return GlobFileSensor(
        task_id=f'{data_type}_file_sensor',
        filepath=filepath,
        base_path=Variable.get('output_path'),
        poke_interval=60,
        dag=dag
    )