from google.cloud import bigquery
from google.oauth2 import service_account

from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

def append_to_bigquery(filename, table_id, **kwargs):
    key_path = Variable.get('api_key_path')
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=['https://www.googleapis.com/auth/bigquery'])

    client = bigquery.Client(credentials=credentials, project=Variable.get('project_name'))
    table_id = '.'.join([Variable.get('project_name'), Variable.get('database_name'), table_id])
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)   

    file_path = Variable.get('output_path') + filename
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()

    errors = job.errors
    if errors: 
        raise AirflowException("Loading command failed", errors) 

def build_load_task(dag, exported_filename, table_id):
    return PythonOperator(
            task_id='load_' + table_id + '_task',
            python_callable=append_to_bigquery,
            op_kwargs={'filename': exported_filename, 'table_id': table_id},
            dag=dag,
        )