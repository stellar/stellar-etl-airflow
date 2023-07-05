from airflow.models.variable import Variable
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from stellar_etl_airflow.default import alert_after_max_retries

cluster = Variable.get("cluster_fields", deserialize_json=True)
project_check = Variable.get("project_check")
dataset_check = Variable.get("dataset_check")
dbt_project = Variable.get("dbt_mart_dataset")
key_path = Variable.get("api_key_path")
credentials = service_account.Credentials.from_service_account_file(key_path)


def check_table(**kwargs):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_ref = f"{kwargs.get('project')}.{kwargs.get('dataset')}.{kwargs.get('table')}"
    try:
        table_id = client.get_table(table_ref)
        return kwargs.get("first_task")
    except:
        return kwargs.get("second_task")


def get_view_query(project_id, dataset_id, table_id, sandbox_dataset):
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    query = table.view_query
    modified_query = query.replace(dataset_id, sandbox_dataset)
    # check if the project is test or prod
    if project_id != project_check:
        modified_query = modified_query.replace(project_check, project_id)
        modified_query = modified_query.replace(dataset_check, dataset_id)
    return modified_query


def build_bq_insert_job_sandbox(
    dag, project, dataset, table, sandbox_dataset, view=False
):
    CREATE_TABLES = (
        f"CREATE OR REPLACE TABLE {project}.{sandbox_dataset}.{table} "
        f"PARTITION BY DATE_TRUNC(batch_run_date, MONTH) "
        f"OPTIONS (partition_expiration_days=180) AS ("
        f"SELECT * "
        f"FROM {project}.{dataset}.{table} "
        f"WHERE batch_run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))"
    )
    if view:
        query = get_view_query(project, dataset, table, sandbox_dataset)
        CREATE_TABLES = f"CREATE OR REPLACE VIEW {sandbox_dataset}.{table} AS ({query})"
    if dataset == dbt_project:
        CREATE_TABLES = (
            f"CREATE OR REPLACE VIEW {project}.{sandbox_dataset}.{table} AS ("
            f"SELECT * FROM {project}.{dataset}.{table})"
        )
    return BigQueryInsertJobOperator(
        project_id=project,
        task_id=f"create_{table}",
        on_failure_callback=alert_after_max_retries,
        configuration={
            "query": {
                "query": CREATE_TABLES,
                "useLegacySql": False,
            }
        },
    )


def build_bq_update_table(dag, project, dataset, table, sandbox_dataset):
    UPDATE_QUERY = (
        f"INSERT INTO {project}.{sandbox_dataset}.{table} "
        f"SELECT * "
        f"FROM {project}.{dataset}.{table} "
        f"WHERE date_trunc(batch_run_date, MONTH) = date_trunc(CURRENT_DATE() - INTERVAL 1 MONTH, MONTH) "
    )
    return BigQueryInsertJobOperator(
        project_id=project,
        task_id=f"update_{table}",
        on_failure_callback=alert_after_max_retries,
        configuration={
            "query": {
                "query": UPDATE_QUERY,
                "useLegacySql": False,
            }
        },
    )


def build_check_table(dag, project, dataset, table, first_task, second_task):
    return BranchPythonOperator(
        task_id=f"check_table_{table}",
        python_callable=check_table,
        op_kwargs={
            "project": project,
            "dataset": dataset,
            "table": table,
            "first_task": first_task,
            "second_task": second_task,
        },
        provide_context=True,
        dag=dag,
    )
