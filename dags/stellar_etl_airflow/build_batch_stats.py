from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 


def build_batch_stats(dag, table):
    PROJECT_ID = Variable.get('bq_project')
    DATASET_ID = 'batch_stats_internal'
    start_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["start"] }}'
    end_ledger = '{{ ti.xcom_pull(task_ids="get_ledger_range_from_times")["end"] - 1 }}'
    batch_id = '{{ run_id }}'
    batch_run_date = '{{ prev_execution_date.to_datetime_string() }}'
    batch_start = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


    INSERT_ROWS_QUERY = (
        f"INSERT {DATASET_ID}.history_archives_dag_runs VALUES "
        f"('{batch_id}', '{batch_run_date}', {start_ledger}, {end_ledger}, '{table}', '{batch_start}');"
    )

    return BigQueryInsertJobOperator(
        project_id=PROJECT_ID,
        task_id=f"insert_batch_stats_{table}",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        }
    )
    