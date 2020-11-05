'''
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into BigQuery.
'''

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema

def build_gcs_to_bq_task(dag, data_type):
    '''
    Creates a task to load a file from Google Cloud Storage into BigQuery. 
    The name of the file being loaded is retrieved through Airflow's Xcom.
    Data types should be: 'ledgers', 'operations', 'trades', 'transactions', or 'factEvents'.

    Parameters:
        dag - parent dag that the task will be attached to 
        data_type - type of the data being uploaded; should be string
    Returns:
        the newly created task
    '''
    
    bucket_name = Variable.get('gcs_exported_data_bucket_name')
    project_name = Variable.get('bq_project')
    dataset_name = Variable.get('bq_dataset')
    table_ids = Variable.get('table_ids', deserialize_json=True)
    prev_task_id = f'load_{data_type}_to_gcs'
    schema_fields = read_local_schema(data_type)
    return GoogleCloudStorageToBigQueryOperator(
        task_id=f'send_{data_type}_to_bq',
        google_cloud_storage_conn_id='google_cloud_platform_connection',
        bigquery_conn_id='google_cloud_platform_connection',
        bucket=bucket_name,
        schema_fields=schema_fields,
        autodetect=False,
        source_format='NEWLINE_DELIMITED_JSON',
        source_objects=["{{ task_instance.xcom_pull(task_ids='"+ prev_task_id +"') }}"],
        destination_project_dataset_table=f'{project_name}.{dataset_name}.{table_ids[data_type]}',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag,
    )