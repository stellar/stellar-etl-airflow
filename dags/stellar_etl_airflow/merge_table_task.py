import json

from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from google.cloud import bigquery
from google.oauth2 import service_account



def read_gcs_schema(data_type):
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_platform_connection')
    schema_fields = json.loads(gcs_hook.download(
        Variable.get('gcs_bucket_name'),
        f'schemas/{data_type}_schema.json').decode("utf-8"))
    return schema_fields

def generate_strings_from_schema(schema, source_table_alias='S'):
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
    dataset_name = Variable.get('bq_dataset')
    true_table_id = Variable.get('table_ids')[data_type]

    equality_field = ''
    if data_type == 'accounts':
        equality_field = 'account_id'
    elif data_type == 'offers':
        equality_field = 'offer_id'
    elif data_type == 'trustlines':
        equality_field = ''
    else:
        raise AirflowException("Unable to write query: unknown data type ", data_type)
    insert_string, update_string = generate_strings_from_schema(schema_fields)
    query = f'''MERGE {dataset_name}.{true_table_id} T
                USING {dataset_name}.{temp_table_id} S
                ON T.{equality_field} = S.{equality_field}
                WHEN MATCHED AND S.deleted THEN
                    DELETE
                WHEN MATCHED THEN
                    {update_string}
                WHEN NOT MATCHED THEN
                    {insert_string}'''
    return query

def apply_gcs_changes(data_type, **kwargs):
    '''
    https://cloud.google.com/bigquery/external-data-cloud-storage#temporary-tables
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

    # Make an API request.
    query_job = client.query(sql, job_config=job_config)

    print(query_job.total_bytes_billed)


def build_apply_gcs_changes_to_bq_task(dag, data_type):
    return PythonOperator(
        task_id='apply_' + data_type + '_changes_to_bq',
        python_callable=apply_gcs_changes,
        op_kwargs={'data_type': data_type},
        dag=dag,
        provide_context=True,
    )
