from datetime import datetime

from airflow import DAG
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import get_default_dag_args

dag = DAG(
    "dbt_sdf_marts",
    default_args=get_default_dag_args(),
    start_date=datetime(2024, 1, 26, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 13 * * *",  # Runs at 13:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["dbt-sdf-marts"],
)

# Wait on ingestion DAGs
wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables"
)

wait_on_partner_pipeline_dag = build_cross_deps(
    dag, "wait_on_partner_pipeline_dag", "partner_pipeline_dag"
)

# DBT models to run
ohlc_task = dbt_task(dag, tag="ohlc")
liquidity_pool_trade_volume_task = dbt_task(dag, tag="liquidity_pool_trade_volume")

mgi_task = dbt_task(dag, tag="mgi")
liquidity_providers_task = dbt_task(dag, tag="liquidity_providers")
trade_agg_task = dbt_task(dag, tag="trade_agg")
fee_stats_agg_task = dbt_task(dag, tag="fee_stats")
asset_stats_agg_task = dbt_task(dag, tag="asset_stats")
network_stats_agg_task = dbt_task(dag, tag="network_stats")
partnership_assets_task = dbt_task(dag, tag="partnership_assets")
history_assets = dbt_task(dag, tag="history_assets")

# DAG task graph
wait_on_dbt_enriched_base_tables >> ohlc_task >> liquidity_pool_trade_volume_task

wait_on_dbt_enriched_base_tables >> mgi_task
wait_on_partner_pipeline_dag >> mgi_task

wait_on_dbt_enriched_base_tables >> liquidity_providers_task
wait_on_dbt_enriched_base_tables >> trade_agg_task
wait_on_dbt_enriched_base_tables >> fee_stats_agg_task
wait_on_dbt_enriched_base_tables >> asset_stats_agg_task
wait_on_dbt_enriched_base_tables >> network_stats_agg_task
wait_on_dbt_enriched_base_tables >> partnership_assets_task
wait_on_dbt_enriched_base_tables >> history_assets
