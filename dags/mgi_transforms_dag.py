from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()
dag = DAG(
    "mgi_transforms",
    default_args=get_default_dag_args(),
    start_date=datetime(2023, 5, 22, 0, 0),
    description="This DAG runs dbt to create the mgi cash in and cash out fact and dimension tables.",
    schedule_interval="30 15 * * *",  # Daily 15:30 UTC after MGI pipeline
    render_template_as_native_obj=True,
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
)
# create dependency on raw data load
wait_on__mgi_dag = build_cross_deps(
    dag, "wait_on_mgi_pipeline", "partner_pipeline_dag", time_delta=10
)

# build snapshot table for raw transactions
snapshot_raw_mgi_stellar_transactions = build_dbt_task(
    dag, "snapshot_raw_mgi_stellar_transactions", "snapshot"
)
# tasks for staging tables for mgi transactions
stg_mgi_transactions_snap = build_dbt_task(dag, "stg_mgi_transactions_snapshot")
stg_mgi_transactions_null_id = build_dbt_task(dag, "stg_mgi_transactions_null_id")
stg_country_code = build_dbt_task(dag, "stg_country_code")
stg_partnership_assets_prices = build_dbt_task(dag, "stg_partnership_assets_prices")
# tasks for fct_mgi_cashflow
int_mgi_transactions_transformed = build_dbt_task(
    dag, "int_mgi_transactions_transformed"
)
int_mgi_transactions_null_id = build_dbt_task(dag, "int_mgi_transactions_null_id")
fct_mgi_cashflow = build_dbt_task(dag, "fct_mgi_cashflow")

# tasks for dim wallets
dim_mgi_wallets = build_dbt_task(dag, "dim_mgi_wallets")

# task for dim dates
dim_dates = build_dbt_task(dag, "dim_dates")

# tasks for mgi monthly usd balance
mgi_monthly_usd_balance = build_dbt_task(dag, "mgi_monthly_usd_balance")

# tasks for network stats
enriched_history_mgi_operations = build_dbt_task(dag, "enriched_history_mgi_operations")
mgi_network_stats_agg = build_dbt_task(dag, "mgi_network_stats_agg")

# DAG task graph
wait_on__mgi_dag >> snapshot_raw_mgi_stellar_transactions
(
    snapshot_raw_mgi_stellar_transactions
    >> stg_mgi_transactions_snap
    >> stg_mgi_transactions_null_id
    >> stg_country_code
    >> stg_partnership_assets_prices
    >> int_mgi_transactions_transformed
    >> int_mgi_transactions_null_id
    >> dim_dates
    >> fct_mgi_cashflow
)
(
    stg_mgi_transactions_snap
    >> dim_mgi_wallets
    >> enriched_history_mgi_operations
    >> mgi_network_stats_agg
)
(dim_dates >> mgi_monthly_usd_balance)
