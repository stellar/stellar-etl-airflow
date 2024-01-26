from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args

dag = DAG(
    "dbt_daily",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 1, 23, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 13 * * *",  # Runs at 13:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["dbt-daily"],
)

# Wait on ingestion DAGs
wait_on_eho_and_current_state = build_cross_deps(
    dag, "wait_on_eho_and_current_state", "dbt_half_hourly"
)

# DBT models to run
ohlc_task = dbt_task(dag, tag="ohlc")
liquidity_pool_trade_volume_task = dbt_task(dag, tag="liquidity_pool_trade_volume")
mgi_task = dbt_task(dag, tag="mgi")
liquidity_providers_task = dbt_task(dag, tag="liquidity_providers")
trade_agg_task = dbt_task(dag, tag="trade_agg")
fee_stats_agg_task = dbt_task(dag, tag="fee_stats_agg")
asset_stats_agg_task = dbt_task(dag, tag="asset_stats_agg")
network_stats_agg_task = dbt_task(dag, tag="network_stats_agg")
partnership_assets_task = dbt_task(dag, tag="partnership_assets")

# DAG task graph
wait_on_eho_and_current_state >> ohlc_task >> liquidity_pool_trade_volume_task
wait_on_eho_and_current_state >> mgi_task
wait_on_eho_and_current_state >> liquidity_providers_task
wait_on_eho_and_current_state >> trade_agg_task
wait_on_eho_and_current_state >> fee_stats_agg_task
wait_on_eho_and_current_state >> asset_stats_agg_task
wait_on_eho_and_current_state >> network_stats_agg_task
wait_on_eho_and_current_state >> partnership_assets_task
