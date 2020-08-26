from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from airflow.models import Variable
from airflow import AirflowException

def append_to_bigquery(file_path, table_id):
    client = bigquery.Client()
    table_id = Variable.get('project_name') + Variable.get('database_name') + table_id
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)   

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    job.result()

    errors = job.errors
    if errors: 
        raise AirflowException("Loading command failed", errors) 

def build_load_task(dag, exported_file, table_id):
    return PythonOperator(
            task_id='load_' + table_id + '_task',
            python_callable=append_to_bigquery,
            op_kwargs={'file_path': exported_file, 'table_id': table_id},
            provide_context=True,
            dag=dag,
        )