'''
This file contains functions for creating Airflow tasks to load files from Google Cloud Storage into BigQuery.
'''
from datetime import timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from stellar_etl_airflow.build_apply_gcs_changes_to_bq_task import read_local_schema

def build_gcs_to_bq_task(dag, export_task_id, project, dataset, data_type, source_object_suffix, partition, cluster):
    '''
    Creates a task to load a file from Google Cloud Storage into BigQuery. 
    The name of the file being loaded is retrieved through Airflow's Xcom.
    Data types should be: 'ledgers', 'operations', 'trades', 'transactions', or 'factEvents'.

    Parameters:
        dag - parent dag that the task will be attached to 
        export_task_id - id of export task that this should consume the XCOM return value of
        source_object_suffix - string, suffix of source object path
        partition - bool if the table is partitioned
    Returns:
        the newly created task
    '''

    bucket_name = Variable.get('gcs_exported_data_bucket_name')
    if cluster:
        cluster_fields = Variable.get('cluster_fields', deserialize_json=True)
        cluster_fields = cluster_fields[data_type]
    else:
        cluster_fields = None
    project_name = project
    if dataset == Variable.get('public_dataset'):
        dataset_type = 'pub'
    else:
        dataset_type = 'bq'
    dataset_name = dataset
    time_partition = {}
    if partition:
        time_partition['type'] = 'MONTH'
        time_partition['field'] = 'batch_run_date'
    staging_table_suffix = ''
    if data_type in ['ledgers', 'assets', 'transactions', 'operations', 'trades']:
        schema_fields = read_local_schema(f'history_{data_type}')
        if data_type == 'assets':
            staging_table_suffix = '_stg'
    else:
        schema_fields = read_local_schema(f'{data_type}')

    return GCSToBigQueryOperator(
        task_id=f'send_{data_type}_to_{dataset_type}',
        execution_timeout=timedelta(seconds=Variable.get('task_timeout', deserialize_json=True)[build_gcs_to_bq_task.__name__]),
        bucket=bucket_name,
        schema_fields=schema_fields,
        autodetect=False,
        source_format='NEWLINE_DELIMITED_JSON',
        source_objects=["{{ task_instance.xcom_pull(task_ids='"+ export_task_id +"')[\"output\"] }}" + source_object_suffix],
        destination_project_dataset_table=f'{project_name}.{dataset_name}.{data_type}{staging_table_suffix}',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        max_bad_records=10,
        time_partitioning=time_partition,
        cluster_fields=cluster_fields,
        dag=dag,
    )

