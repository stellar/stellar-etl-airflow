import os
import logging
from google.oauth2 import service_account

from airflow import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from apiclient.http import MediaFileUpload
from googleapiclient import errors
from googleapiclient.discovery import build
from stellar_etl_airflow.build_export_task import parse_ledger_range

MEGABYTE = 1024 * 1024

def build_storage_credentials():
    key_path = Variable.get('api_key_path')
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return build('storage', 'v1', credentials=credentials, cache_discovery=False)

def attempt_upload(local_filepath, gcs_filepath, bucket_name, mime_type='text/plain'):
    '''
    Tries to upload the file into Google Cloud Storage. Files above 10 megabytes are uploaded incrementally
    as described here: https://github.com/googleapis/google-api-python-client/blob/master/docs/media.md#resumable-media-chunked-upload.

    
    Parameter:
        local_filepath - path to the local file to be uploaded 
        gcs_filepath - path for the file in gcs
        bucket_name - name of the bucket to upload to
        mime_type - type of media to upload
    Returns:
        True if the upload is successful. Raises an Airflow error otherwise
    '''
    storage_service = build_storage_credentials()
    if os.path.getsize(local_filepath) > 10 * MEGABYTE:
        media = MediaFileUpload(local_filepath, mime_type, resumable=True)

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

        try:
            storage_service.objects().insert(bucket=bucket_name, name=gcs_filepath, media_body=media).execute()
            return True
        except errors.HttpError as e:
            raise AirflowException("Unable to upload file to gcs", e)

def upload_to_gcs(filename, data_type, **kwargs):
    '''
    Uploads a local file to Google Cloud Storage and deletes the local file if the upload is sucessful.

    
    Parameter:
        filename - name of the file to be uploaded 
        data_type - type of the data being uploaded (transaction, ledger, etc)
    Returns:
        N/A
    '''
    start_ledger, end_ledger = parse_ledger_range(kwargs)
    gcs_filepath = 'exported/' + data_type + '/' + '-'.join([start_ledger, end_ledger, filename])
    local_filepath = Variable.get('output_path') + filename
    bucket_name = Variable.get('gcs_bucket_name')

    success = attempt_upload(local_filepath, gcs_filepath, bucket_name)
    if success:
        os.remove(local_filepath)

def build_load_task(dag, data_type, exported_filename):
    return PythonOperator(
            task_id='load_' + data_type + '_to_gcs',
            python_callable=upload_to_gcs,
            op_kwargs={'filename': exported_filename, 'data_type': data_type},
            dag=dag,
            provide_context=True,
        )