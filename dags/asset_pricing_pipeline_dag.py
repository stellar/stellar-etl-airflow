import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "asset_pricing",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 6, 1),
    description="This DAG runs dbt to calculate asset pricing based on stablecoin and XLM trades",
    # leave gap for data to be filled for midnight/noon in trades table
    schedule_interval="0 2,14 * * *",  # Twice daily at 2am,2pm
    params={},
)

# tasks for staging tables for trades
stg_history_trades = build_dbt_task(dag, "stg_history_trades")
stg_asset_prices_usd = build_dbt_task(dag, "stg_asset_prices_usd")

# tasks for ohlc intermediate tables
int_stable_coin_prices = build_dbt_task(dag, "int_stable_coin_prices")
int_usdc_trades = build_dbt_task(dag, "int_usdc_trades")
int_usds_trades = build_dbt_task(dag, "int_usds_trades")
int_xlm_trades = build_dbt_task(dag, "int_xlm_trades")

# tasks for final fact
ohlc_exchange_fact = build_dbt_task(dag, "ohlc_exchange_fact")

# DAG task graph
stg_history_trades >> int_stable_coin_prices >> ohlc_exchange_fact
stg_history_trades >> int_usdc_trades >> ohlc_exchange_fact
stg_history_trades >> int_usds_trades >> ohlc_exchange_fact
stg_history_trades >> int_xlm_trades >> ohlc_exchange_fact
stg_asset_prices_usd >> ohlc_exchange_fact
