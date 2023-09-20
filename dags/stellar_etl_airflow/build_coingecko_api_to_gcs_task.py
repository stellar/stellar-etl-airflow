import logging

import pandas as pd
import requests
from google.cloud import storage


def response_to_gcs(bucket_name, endpoint, destination_blob_name, columns):
    response = requests.get(endpoint)
    euro_data = response.json()
    df = pd.DataFrame(euro_data, index=None, columns=columns)
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    euro_data = df.to_csv(index=False)

    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(euro_data, content_type="text/csv")
    logging.info(f"File {destination_blob_name}.txt uploaded to {bucket_name}.")
