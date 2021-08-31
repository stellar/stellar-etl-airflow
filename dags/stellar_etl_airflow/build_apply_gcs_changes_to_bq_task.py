'''
This file contains functions for creating Airflow tasks to merge data on ledger entry changes from
a file in Google Cloud storage into a BigQuery table.
'''
import os
import json
import logging
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from os.path import splitext, basename



def read_local_schema(data_type):
    '''
    Reads the schema file corresponding to data_type and parses it.
    Data types should be: 'accounts', 'assets', 'ledgers', 'offers', 'operations', 'trades', 'transactions', 'trustlines',
    'dimAccounts', 'dimOffers', 'dimMarkets', or 'factEvents'.

    Parameters:
        data_type - type of the data being uploaded; should be string
    Returns:
        the parsed schema as a dictionary
    '''

    # since the dags folder is shared among airflow workers and the webservers, schemas are stored in the dags folder
    schema_filepath = os.path.join(Variable.get('schema_filepath'), f'{data_type}_schema.json') 
    logging.info(f'Loading schema file at {schema_filepath}')
 
    with open(schema_filepath, 'r') as schema_file:
        schema_fields = json.loads(schema_file.read())

    return schema_fields

def generate_insert_query(schema, source_table_alias):
    '''
    Generates the SQL insert query that will insert a row in a table based on the provided schema.
    
    Parameters:
        schema - an array of dictionaries, each containing the name, type, and description of a schema field 
        source_table_alias - the name of the table being used as a data source
    Returns:
        the insert query
    '''
    # trades have an order field, which is also a SQL keyword. BigQuery interprets `order` as the column
    for field in schema:
        if field['name'] == 'order':
            field['name'] = '`order`'

    insert_list = ', '.join([field['name'] for field in schema])
    return f'INSERT ({insert_list}) VALUES ({insert_list})'

def generate_update_query(schema, source_table_alias):
    '''
    Generates the SQL update query that will update a row in a table based on the provided schema.
    
    Parameters:
        schema - an array of dictionaries, each containing the name, type, and description of a schema field 
        source_table_alias - the name of the table being used as a data source
    Returns:
        the update query
    '''

    update_list = ', '.join([f'{field["name"]} = {source_table_alias}.{field["name"]}' for field in schema])
    return f'UPDATE SET {update_list}'

def generate_equality_comparison(data_type, source_table_alias, dest_table_alias):
    '''
    Generates the equality comparison used to determine if two rows are the same.
    Data types should be: 'accounts', 'offers', 'trustlines', 'ledgers', 'transactions', 'operations',
    'trades', 'assets', 'dimAccounts', 'dimOffers', or 'dimMarkets'. 
    Parameters:
        data_type - type of the data being uploaded; should be string
        source_table_alias - the name of the table being used as a data source
        dest_table_alias - the name of the table being used as a destination
    Returns:
        the update query
    '''

    equality_comparison = ''
    switch = {
        'accounts': f'{dest_table_alias}.account_id = {source_table_alias}.account_id',
        'offers': f'{dest_table_alias}.offer_id = {source_table_alias}.offer_id',
        'trustlines': f'{dest_table_alias}.account_id = {source_table_alias}.account_id AND {dest_table_alias}.asset_type = {source_table_alias}.asset_type \
        AND {dest_table_alias}.asset_issuer = {source_table_alias}.asset_issuer AND {dest_table_alias}.asset_code = {source_table_alias}.asset_code',
        'dimAccounts': f'{dest_table_alias}.account_id = {source_table_alias}.account_id',
        'dimOffers': f'{dest_table_alias}.dim_offer_id = {source_table_alias}.dim_offer_id',
        'dimMarkets': f'{dest_table_alias}.market_id = {source_table_alias}.market_id',
        'ledgers': f'{dest_table_alias}.ledger_hash = {source_table_alias}.ledger_hash',
        'transactions': f'{dest_table_alias}.transaction_hash = {source_table_alias}.transaction_hash',
        'operations': f'{dest_table_alias}.id = {source_table_alias}.id',
        'trades': f'{dest_table_alias}.history_operation_id = {source_table_alias}.history_operation_id AND {dest_table_alias}.order = {source_table_alias}.order',
        'assets': f'{dest_table_alias}.id = {source_table_alias}.id',
        'factEvents': f'{dest_table_alias}.offer_instance_id = {source_table_alias}.offer_instance_id AND {dest_table_alias}.ledger_id = {source_table_alias}.ledger_id '
    }

    equality_comparison = switch.get(data_type, 'No comparison')
    if equality_comparison == 'No comparison':
        raise AirflowException("Unable to write query: unknown data type for merges ", data_type)
    return equality_comparison

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
    project_name = Variable.get('bq_project')
    dataset_name = Variable.get('bq_dataset')
    true_table_id = Variable.get('table_ids', deserialize_json=True)[data_type]
    dest_alias = 'T'
    source_alias = 'S'   

    insert_query = generate_insert_query(schema_fields, source_alias)
    update_query = generate_update_query(schema_fields, source_alias)
    equality_comparison = generate_equality_comparison(data_type, source_alias, dest_alias)

    query = f'''MERGE `{project_name}.{dataset_name}.{true_table_id}` {dest_alias}
                USING `{temp_table_id}` {source_alias}
                ON {equality_comparison}
                WHEN MATCHED AND {source_alias}.deleted THEN
                    DELETE
                WHEN MATCHED THEN
                    {update_query}
                WHEN NOT MATCHED THEN
                    {insert_query}'''

    return query

def create_insert_unique_query(temp_table_id, data_type, schema_fields):
    '''
    Creates the string representation of the insert unique query. Data types should be: 'ledgers', 
    'transactions', 'operations', 'trades', 'assets', 'dimAccounts', 'dimOffers', or 'dimMarkets'. 
    
    Parameters:
        temp_table_id - the id of the temporary table where the external data is located 
        data_type - type of the data being uploaded; should be string
        schema_fields - the schema fields for the external data
    Returns:
        the newly created task
    '''
    project_name = Variable.get('bq_project')
    dataset_name = Variable.get('bq_dataset')
    true_table_id = Variable.get('table_ids', deserialize_json=True)[data_type]
    dest_alias = 'T'
    source_alias = 'S'   

    insert_query = generate_insert_query(schema_fields, source_alias)
    equality_comparison = generate_equality_comparison(data_type, source_alias, dest_alias)

    query = f'''MERGE `{project_name}.{dataset_name}.{true_table_id}` {dest_alias}
                USING `{temp_table_id}` {source_alias}
                ON {equality_comparison}
                WHEN NOT MATCHED THEN
                    {insert_query}'''

    return query

def apply_gcs_changes(data_type, **kwargs):
    '''
    Sets up a file in Google Cloud Storage as an temporary table, and merges it with an existing table in BigQuery.
    The file's location in GCS is retrieved through XCOM, and the schema for the temporary table is loaded from GCS.
    Data types should be: 'accounts', 'offers', 'trustlines', 'ledgers', 'transactions', 'operations',
    'trades', 'assets', 'dimAccounts', 'dimOffers', or 'dimMarkets'. 

    Parameters:
        data_type - type of the data being uploaded; should be string
    Returns:
        N/A
    '''

    key_path = Variable.get('api_key_path')
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    gcs_bucket_name = Variable.get('gcs_exported_data_bucket_name')
    gcs_filepath = kwargs['task_instance'].xcom_pull(task_ids=f'load_{data_type}_to_gcs')
    schema_dict = read_local_schema(data_type)
    bq_schema_list = [bigquery.SchemaField.from_api_repr(field) for field in schema_dict]

    external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
    external_config.source_uris = [f'gs://{gcs_bucket_name}/{gcs_filepath}']
    external_config.schema = bq_schema_list

    # The temporary table is is equal to the name of the file used to create it. Table ids cannot have '-'. Instead they have '_'
    table_id = f'{splitext(basename(gcs_filepath))[0]}'
    table_id = table_id.replace('-', '_')

    job_config = bigquery.QueryJobConfig(
        table_definitions={table_id: external_config}
    )

    #check if the table already exists; if it does not then we need to create it using the schema that we have already read in
    true_table_id = f'{Variable.get("bq_project")}.{Variable.get("bq_dataset")}.{Variable.get("table_ids", deserialize_json=True)[data_type]}'
    try:
        _ = client.get_table(true_table_id)
    except NotFound:
        logging.info(f'Bigquery table not found; creating table with id {true_table_id}')
        table = bigquery.Table(true_table_id, schema=bq_schema_list)
        client.create_table(table)

    if data_type in ['accounts', 'offers', 'trustlines']:
        logging.info('Using merge query...')
        sql_query = create_merge_query(table_id, data_type, schema_dict)
    elif data_type in ['ledgers', 'transactions', 'operations', 'trades', 'assets', 'dimAccounts', 'dimOffers', 'dimMarkets', 'factEvents']:
        logging.info('Using insert unique query...')
        sql_query = create_insert_unique_query(table_id, data_type, schema_dict)
    else:
        raise AirflowException(f'Data type {data_type} has no corresponding query')

    logging.info(f'Query is: {sql_query}')
    logging.info(f'Running BigQuery job with config: {job_config._properties}')
    query_job = client.query(sql_query, job_config=job_config)
    result_rows = query_job.result()

    if query_job.error_result:
        logging.info(f'Query errors: {query_job.errors}')
        raise AirflowException(f'Query job failed: {query_job.error_result}')

    logging.info(f'Job timeline: {[t._properties for t in query_job.timeline]}')
    logging.info(f'{query_job.total_bytes_billed} bytes billed at billing tier {query_job.billing_tier}')
    logging.info(f'Total rows affected: {query_job.num_dml_affected_rows}')

def build_apply_gcs_changes_to_bq_task(dag, data_type):
    '''
    Creates a task that applies changes from a Google Cloud Storage file to a BigQuery table.
    Data types should be: 'accounts', 'offers', 'trustlines', 'ledgers', 'transactions', 'operations',
    'trades', 'assets', 'dimAccounts', 'dimOffers', or 'dimMarkets'. 
    
    Parameters:
        dag - the parent dag
        data_type - type of the data being uploaded; should be string
    Returns:
        the newly created task
    '''

    return PythonOperator(
        task_id='apply_' + data_type + '_changes_to_bq',
        python_callable=apply_gcs_changes,
        op_kwargs={'data_type': data_type},
        dag=dag,
        provide_context=True,
    )
