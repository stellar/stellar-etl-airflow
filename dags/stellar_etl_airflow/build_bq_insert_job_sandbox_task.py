from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
from stellar_etl_airflow.default import alert_after_max_retries


def check_view(**kwargs):
    client = bigquery.Client()
    table_ref = f"{kwargs.get('project')}.{kwargs.get('dataset')}.{kwargs.get('table')}"
    try:
        table_id = client.get_table(table_ref)
        return "end_of_execution"
    except:
        return f"update_{kwargs.get('table')}"


def get_view_query(project_id, dataset_id, table_id, sandbox_dataset):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    query = table.view_query
    modified_query = query.replace(dataset_id, sandbox_dataset)
    return modified_query


def build_bq_insert_job_sandbox(
    dag, project, dataset, table, sandbox_dataset, view=False
):
    UPDATE_TABLES = (
        f"CREATE OR REPLACE TABLE {sandbox_dataset}.{table} AS ( "
        f"SELECT * "
        f"FROM {dataset}.{table} "
        f"WHERE batch_run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))"
    )

    if view:
        query = get_view_query(project, dataset, table, sandbox_dataset)
        UPDATE_TABLES = f"CREATE OR REPLACE VIEW {sandbox_dataset}.{table} AS ({query})"

    return BigQueryInsertJobOperator(
        project_id=project,
        task_id=f"update_{table}",
        on_failure_callback=alert_after_max_retries,
        configuration={
            "query": {
                "query": UPDATE_TABLES,
                "useLegacySql": False,
            }
        },
    )


def build_check_view(dag, project, dataset, table):
    return BranchPythonOperator(
        task_id=f"check_view_{table}",
        python_callable=check_view,
        op_kwargs={"project": project, "dataset": dataset, "table": table},
        provide_context=True,
        dag=dag,
    )
