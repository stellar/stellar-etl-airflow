import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "marts_tables",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2015, 9, 30),
    description="This DAG runs dbt to create the tables for the models in marts/ but not any marts subdirectories.",
    schedule_interval="0 17 * * *",  # Daily 11 AM UTC
    params={},
    catchup=True,
    max_active_runs=1,
)

wait_on_partnership_assets_dag = build_cross_deps(
    dag, "wait_on_partnership_assets_pipeline", "partnership_assets", time_delta=10
)

# tasks for staging tables for marts
stg_history_transactions = build_dbt_task(dag, "stg_history_transactions")
stg_history_assets = build_dbt_task(dag, "stg_history_assets")
stg_history_trades = build_dbt_task(dag, "stg_history_trades")

# tasks for intermediate trades tables
int_trade_agg_day = build_dbt_task(dag, "int_trade_agg_day")
int_trade_agg_month = build_dbt_task(dag, "int_trade_agg_month")
int_trade_agg_week = build_dbt_task(dag, "int_trade_agg_week")
int_trade_agg_year = build_dbt_task(dag, "int_trade_agg_year")

# tasks for intermediate asset stats tables
int_meaningful_asset_prices = build_dbt_task(dag, "int_meaningful_asset_prices")
int_asset_stats_agg = build_dbt_task(dag, "int_asset_stats_agg")
stg_excluded_accounts = build_dbt_task(dag, "stg_excluded_accounts")
stg_xlm_to_usd = build_dbt_task(dag, "stg_xlm_to_usd")

# tasks for marts tables
network_stats_agg = build_dbt_task(dag, "network_stats_agg")
asset_stats_agg = build_dbt_task(dag, "asset_stats_agg")
fee_stats_agg = build_dbt_task(dag, "fee_stats_agg")
history_assets = build_dbt_task(dag, "history_assets")
trade_agg = build_dbt_task(dag, "trade_agg")
liquidity_providers = build_dbt_task(dag, "liquidity_providers")

# DAG task graph
# graph for marts tables
network_stats_agg
liquidity_providers

wait_on_partnership_assets_dag >> int_meaningful_asset_prices >> int_asset_stats_agg
stg_excluded_accounts >> int_asset_stats_agg
stg_xlm_to_usd >> int_asset_stats_agg
int_asset_stats_agg >> asset_stats_agg
stg_history_transactions >> fee_stats_agg

stg_history_assets >> history_assets

stg_history_trades >> int_trade_agg_day >> trade_agg
stg_history_trades >> int_trade_agg_month >> trade_agg
stg_history_trades >> int_trade_agg_week >> trade_agg
stg_history_trades >> int_trade_agg_year >> trade_agg
