'''
This file contains functions for creating Airflow tasks to merge data on ledger entry changes from
a file in Google Cloud storage into a BigQuery table.
'''
import json

from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from google.cloud import bigquery
from google.oauth2 import service_account



def read_gcs_schema(data_type):
    '''
    Reads a the schema file corresponding to data_type from Google Cloud Storage and parses it.
    Data types should be: 'accounts', 'ledgers', 'offers', 'operations', 'trades', 'transactions', or 'trustlines'.

    Parameters:
        data_type - type of the data being uploaded; should be string 
    Returns:
        the parsed schema
    '''

    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_platform_connection')
    schema_fields = json.loads(gcs_hook.download(
        Variable.get('gcs_bucket_name'),
        f'schemas/{data_type}_schema.json').decode("utf-8"))
    return schema_fields

def generate_queries_from_schema(schema, source_table_alias):
    '''
    Generates the SQL queries that will insert or update a row in a table based on the provided schema.
    
    Parameters:
        schema - an array of dictionaries, each containing the name, type, and description of a schema field 
        source_table_alias - the name of the table being used as a data source
    Returns:
        the insert query and update query
    '''

    insert = []
    update = []
    for field in schema:
        name = field['name']
        insert.append(name)
        update.append(f'{name} = {source_table_alias}.{name}')
    insert_list = ', '.join(insert)
    updated_list = ', '.join(update)
    return f'INSERT ({insert_list}) VALUES ({insert_list})', f'UPDATE SET {updated_list}'

def create_merge_query(temp_table_id, data_type, schema_fields):
    '''
    Creates the string representation of the merge query. Data types should be: 'accounts', 'offers', or 'trustlines'. 
    
    Parameters:
        temp_table_id - the id of the temporary table where the external data is located 
        data_type - type of the data being uploaded; should be string
        schema_fields - the schema fields for the external data
    Returns:
        the newly created task
    '''

    dataset_name = Variable.get('bq_dataset')
    true_table_id = Variable.get('table_ids')[data_type]
    dest_alias = 'T'
    source_alias = 'S'
    
    equality_comparison = ''
    if data_type == 'accounts':
        equality_comparison = f'{dest_alias}.account_id = {source_alias}.account_id'
    elif data_type == 'offers':
        equality_comparison = f'{dest_alias}.offer_id = {source_alias}.offer_id'
    elif data_type == 'trustlines':
        equality_comparison = f'{dest_alias}.account_id = {source_alias}.account_id AND {dest_alias}.asset_type = {source_alias}.asset_type AND {dest_alias}.asset_issuer = {source_alias}.asset_issuer AND {dest_alias}.asset_code = {source_alias}.asset_code'
    else:
        raise AirflowException("Unable to write query: unknown data type for merges ", data_type)

    insert_query, update_query = generate_queries_from_schema(schema_fields, source_alias)

    query = f'''MERGE {dataset_name}.{true_table_id} {dest_alias}
                USING {dataset_name}.{temp_table_id} {source_alias}
                ON {equality_comparison}
                WHEN MATCHED AND {source_alias}.deleted THEN
                    DELETE
                WHEN MATCHED THEN
                    {update_query}
                WHEN NOT MATCHED THEN
                    {insert_query}'''

    return query

def apply_gcs_changes(data_type, **kwargs):
    '''
    Sets up a file in Google Cloud Storage as an temporary table, and merges it with an existing table in BigQuery.
    The file's location in GCS is retrieved through XCOM, and the schema for the temporary table is loaded from GCS.
    Data types should be: 'accounts', 'offers', or 'trustlines'.

    Parameters:
        data_type - type of the data being uploaded; should be string 
    Returns:
        N/A
    '''

    key_path = Variable.get('api_key_path')
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    gcs_filename = kwargs['task_instance'].xcom_pull(task_ids=f'load_{data_type}_to_gcs')
    schema = read_gcs_schema(data_type)
    external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
    external_config.source_uris = [f'gs://{gcs_filename}']
    external_config.schema = schema
    table_id = f'{data_type}_temp_table'
    job_config = bigquery.QueryJobConfig(table_definitions={table_id: external_config})

    sql = create_merge_query(table_id, data_type, schema)

    query_job = client.query(sql, job_config=job_config)

def build_apply_gcs_changes_to_bq_task(dag, data_type):
    return PythonOperator(
        task_id='apply_' + data_type + '_changes_to_bq',
        python_callable=apply_gcs_changes,
        op_kwargs={'data_type': data_type},
        dag=dag,
        provide_context=True,
    )
