import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "mgi_transforms",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 5, 22, 0, 0),
    description="This DAG runs dbt to create the mgi cash in and cash out fact and dimension tables.",
    schedule_interval="30 15 * * *",  # Daily 15:30 UTC after MGI pipeline
    params={},
    max_active_runs=1,
)

# build snapshot table for raw transactions
snapshot_raw_mgi_stellar_transactions = build_dbt_task(
    dag, "snapshot_raw_mgi_stellar_transactions", "snapshot"
)

# tasks for staging tables for mgi transactions
stg_mgi_transactions_snap = build_dbt_task(dag, "stg_mgi_transactions_snapshot")
stg_mgi_transactions_null_id = build_dbt_task(dag, "stg_mgi_transactions_null_id")
stg_country_code = build_dbt_task(dag, "stg_country_code")

# tasks for fct_mgi_cashflow
int_mgi_transactions_transformed = build_dbt_task(
    dag, "int_mgi_transactions_transformed"
)
int_mgi_transactions_null_id = build_dbt_task(dag, "int_mgi_transactions_null_id")
fct_mgi_cashflow = build_dbt_task(dag, "fct_mgi_cashflow")

# DAG task graph
# graph for partnership_assets__account_holders_activity_fact
(
    snapshot_raw_mgi_stellar_transactions
    >> stg_mgi_transactions_snap
    >> stg_mgi_transactions_null_id
    >> stg_country_code
    >> int_mgi_transactions_transformed
    >> int_mgi_transactions_null_id
    >> fct_mgi_cashflow
)
