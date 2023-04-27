import logging
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from glob import glob
from os import listdir
from shutil import copy, copytree, ignore_patterns
from tempfile import mkdtemp
from typing import List, Tuple

from google.cloud import storage

logging.basicConfig(level=logging.INFO)


def _create_files_list(env: str) -> Tuple[str, List[str]]:
    temp_dir = mkdtemp()

    # do not upload `dags/ddls/` directory and some files that Airflow don't use
    files_to_ignore = ignore_patterns(
        "ddls*", "__pycache__", "__init__.py", "*_test.py", "add_dags_to_composer.py"
    )

    # copy everything but the ignored files to a temp directory
    copytree("dags/", f"{temp_dir}/", ignore=files_to_ignore, dirs_exist_ok=True)
    # copy all schemas
    copytree("schemas/", f"{temp_dir}/", dirs_exist_ok=True)
    # copy airflow configuration file
    copy(f"airflow-{env}.cfg", f"{temp_dir}/airflow.cfg")
    copy(f"airflow-{env}.cfg", f"airflow.cfg")

    dags = glob(f"{temp_dir}/**/*.*", recursive=True)
    return (temp_dir, dags)


def upload_dags_to_composer(bucket_name: str, env: str) -> None:
    """
    Given a directory, this function moves all DAG files from that directory
    to a temporary directory, then uploads all contents of the temporary directory
    to a given cloud storage bucket
    Args:
        dags_directory (str): a fully qualified path to a directory that contains a "dags/" subdirectory
        bucket_name (str): the GCS bucket of the Cloud Composer environment to upload DAGs to
        name_replacement (str, optional): the name of the "dags/" subdirectory that will be used when constructing the temporary directory path name Defaults to "dags/".
    """
    temp_dir, files = _create_files_list(env)

    if len(files) > 0:
        # Note - the GCS client library does not currently support batch requests on uploads
        # if you have a large number of files, consider using
        # the Python subprocess module to run gsutil -m cp -r on your dags
        # See https://cloud.google.com/storage/docs/gsutil/commands/cp for more info
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        for f in files:
            # remove path to temp dir
            blob = bucket.blob(f)
            if f.endswith(".json"):
                # create schemas directory
                f = f.replace(f"{temp_dir}/", "schemas/")
                schema_destination = "dags/" + f
                blob = bucket.blob(schema_destination)
            if f.endswith(".cfg"):
                # insert airflow configuration file
                f = f.replace(f"{temp_dir}/", "")
            else:
                # create dags directory
                f = f.replace(f"{temp_dir}/", "dags/")

            try:
                # Upload to your bucket
                blob.upload_from_filename(f)
                logging.info(f"File {f} uploaded to {bucket_name}/{f}.")
            except FileNotFoundError:
                list_files = listdir(temp_dir)
                logging.error(
                    f"File not found in temporary directory. List of files: {list_files}"
                )
                raise

    else:
        logging.info("No files to upload.")


if __name__ == "__main__":
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Name of the bucket of your Composer environment without the gs:// prefix",
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["prod", "dev"],
        help="Name of the Composer environment. Default value is 'dev'",
    )

    args = parser.parse_args()

    upload_dags_to_composer(args.bucket, args.env)
