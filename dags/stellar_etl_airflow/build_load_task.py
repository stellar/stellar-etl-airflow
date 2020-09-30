'''
This file contains function to build Airflow tasks that load local files into Google Cloud Storage. 
These load tasks become part of larger DAGs in the overall ETL process.
'''

import os
import logging
from google.oauth2 import service_account

from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from googleapiclient.http import MediaFileUpload
from googleapiclient import errors
from googleapiclient.discovery import build
from stellar_etl_airflow.build_export_task import select_correct_filename

def build_storage_service():
    '''
    Creates a storage service object that uses the credentials specified by the Airflow api_key_path variable.
    This v1 storage object is described here: http://googleapis.github.io/google-api-python-client/docs/dyn/storage_v1.html
    Parameters:
        N/A
    Returns:
        storage service object
    '''

    key_path = Variable.get('api_key_path')
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return build('storage', 'v1', credentials=credentials, cache_discovery=False)

def attempt_upload(local_filepath, gcs_filepath, bucket_name, mime_type='text/plain'):
    '''
    Tries to upload the file into Google Cloud Storage. Files above 10 megabytes are uploaded incrementally
    as described here: https://github.com/googleapis/google-api-python-client/blob/master/docs/media.md#resumable-media-chunked-upload

    
    Parameters:
        local_filepath - path to the local file to be uploaded 
        gcs_filepath - path for the file in gcs
        bucket_name - name of the bucket to upload to
        mime_type - type of media to upload; other values are defined https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types
    Returns:
        True if the upload is successful. Raises an Airflow error otherwise
    '''

    storage_service = build_storage_service()
    if os.path.getsize(local_filepath) > 10 * 2 ** 20:
        media = MediaFileUpload(local_filepath, mime_type, resumable=True)
        logging.info('File is large, uploading to GCS in chunks')
        try:
            request = storage_service.objects().insert(bucket=bucket_name, name=gcs_filepath, media_body=media)
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    logging.info("Uploaded %d%%." % int(status.progress() * 100))
            return True
        except errors.HttpError as e:
            raise AirflowException("Unable to upload large file to gcs", e)
    else:
        media = MediaFileUpload(local_filepath, mime_type)
        logging.info('File is small, uploading to GCS all at once')
        try:
            storage_service.objects().insert(bucket=bucket_name, name=gcs_filepath, media_body=media).execute()
            return True
        except errors.HttpError as e:
            raise AirflowException("Unable to upload file to gcs", e)

def upload_to_gcs(data_type, prev_task_id, **kwargs):
    '''
    Uploads a local file to Google Cloud Storage and deletes the local file if the upload is successful.
    Data types should be: accounts, ledgers, offers, operations, trades, transactions, or trustlines.
    
    Parameters:
        data_type - type of the data being uploade; should be string
        prev_task_id - the task id to get the filename from
    Returns:
        the full filepath in Google Cloud Storage of the uploaded file
    '''

    # when getting the filename from the file sensor tasks, we get a string filename
    filename = kwargs['task_instance'].xcom_pull(task_ids=prev_task_id)
    logging.info(type(filename))
    if isinstance(filename, dict):
        filename = filename["output_file"]
    elif isinstance(filename, bytes):
        filename = filename.decode('utf-8')
    logging.info(f'Pulling filename from task {prev_task_id}; result is {filename}')

    gcs_filepath = f'exported/{data_type}/{os.path.basename(filename)}'

    local_filepath = Variable.get('output_path') + filename
    bucket_name = Variable.get('gcs_exported_data_bucket_name')

    logging.info(f'Attempting to upload local file at {local_filepath} to Google Cloud Storage path {gcs_filepath} in bucket {bucket_name}')
    success = attempt_upload(local_filepath, gcs_filepath, bucket_name)
    if success:
        #TODO: consider adding backups or integrity checking before uploading/deleting
        logging.info(f'Upload successful, removing file at {local_filepath}')
        os.remove(local_filepath)
    else: 
        raise AirflowException('Upload was not successful')

    return gcs_filepath

def build_load_task(dag, data_type, prev_task_id):
    '''
    Creates a task that loads a local file into Google Cloud Storage.
    Data types should be: accounts, ledgers, offers, operations, trades, transactions, or trustlines.
    
    Parameters:
        dag - the parent dag
        data_type - type of the data being uploaded; should be string
        prev_task_id - the task id to get the filename from 
    Returns:
        the newly created task
    '''

    return PythonOperator(
            task_id='load_' + data_type + '_to_gcs',
            python_callable=upload_to_gcs,
            op_kwargs={'data_type': data_type, 'prev_task_id': prev_task_id},
            dag=dag,
            provide_context=True,
        )