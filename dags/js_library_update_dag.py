from datetime import datetime
from json import loads

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from stellar_etl_airflow.build_check_latest_tag_task import is_new_tag
from stellar_etl_airflow.build_get_update_library_task import (
    check_file_upload_date,
    send_library_to_gcs,
)
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "js_library_update_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2023, 1, 1),
    description="This DAG updates the js library from bower-js-stellar-base repo.",
    schedule_interval="0 17 * * *",  # Daily 11 AM UTC
    render_template_as_native_obj=True,
    params={
        "alias": "js_library",
    },
    user_defined_filters={"fromjson": lambda s: loads(s)},
    catchup=False,
)

repo = "{{ var.value.stellar_base_js_repositoy_name }}"
file_url = "{{ var.value.stellar_base_js_repository_url }}"
bucket_name = "{{ var.value.package_js_bucket_name }}"
destination_blob_name = "{{ var.value.package_js_object_name }}"

check_last_update = PythonOperator(
    task_id="check_last_update_task",
    python_callable=check_file_upload_date,
    op_kwargs={"bucket_name": bucket_name, "file_name": destination_blob_name},
    dag=dag,
)

check_tag = BranchPythonOperator(
    task_id="check_tag_task",
    python_callable=is_new_tag,
    op_kwargs={
        "repo": repo,
        "last_known_tag_date": "{{ ti.xcom_pull(key='last_update_date',task_ids='check_last_update_task') }}",
    },
    dag=dag,
)

update_library = PythonOperator(
    task_id="update_library_task",
    python_callable=send_library_to_gcs,
    op_kwargs={
        "file_url": file_url,
        "bucket_name": bucket_name,
        "destination_blob_name": destination_blob_name,
    },
    dag=dag,
)

task_aready_up_to_date = EmptyOperator(
    task_id="task_aready_up_to_date_task",
    dag=dag,
)

check_last_update >> check_tag >> [update_library, task_aready_up_to_date]
