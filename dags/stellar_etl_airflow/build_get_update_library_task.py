import logging

import requests
from google.cloud import storage


def download_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Error downloading file: {response.status_code}")


def upload_to_gcs(bucket_name, destination_blob_name, content):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(content)


def send_library_to_gcs(file_url, bucket_name, destination_blob_name):
    try:
        file_content = download_file(file_url)
        upload_to_gcs(
            bucket_name,
            destination_blob_name,
            file_content,
            content_type="application/javascript",
        )
        logging.info(
            f"File {destination_blob_name} loaded successfully to {bucket_name}."
        )
    except Exception as e:
        logging.error(f"Error: {e}")


def check_file_upload_date(bucket_name, file_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    last_update_date = blob.updated
    last_update_date = {"last_update_date": last_update_date}
    return last_update_date
