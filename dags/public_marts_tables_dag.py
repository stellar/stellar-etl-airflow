import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "public_marts_tables",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 4, 4, 0, 0),
    description="This DAG runs public dbt to create the tables for the models in marts/ but not any marts subdirectories.",
    schedule_interval="0 11 * * *",  # Daily 11 AM UTC
    params={},
    catchup=False,
)

# tasks for staging tables for marts
stg_history_transactions = build_dbt_task(dag, "stg_history_transactions")
stg_history_ledgers = build_dbt_task(dag, "stg_history_ledgers")
stg_history_assets = build_dbt_task(dag, "stg_history_assets")
stg_history_trades = build_dbt_task(dag, "stg_history_trades")
stg_history_operations = build_dbt_task(dag, "stg_history_operations")

# tasks for intermediate trades tables
int_trade_agg_day = build_dbt_task(dag, "int_trade_agg_day")
int_trade_agg_month = build_dbt_task(dag, "int_trade_agg_month")
int_trade_agg_week = build_dbt_task(dag, "int_trade_agg_week")
int_trade_agg_year = build_dbt_task(dag, "int_trade_agg_year")

# tasks for marts tables
fee_stats_agg = build_dbt_task(dag, "fee_stats_agg")
history_assets = build_dbt_task(dag, "history_assets")
trade_agg = build_dbt_task(dag, "trade_agg")

# DAG task graph
# graph for marts tables

stg_history_transactions >> fee_stats_agg
stg_history_ledgers >> fee_stats_agg

stg_history_assets >> history_assets

stg_history_trades >> int_trade_agg_day >> trade_agg
stg_history_trades >> int_trade_agg_month >> trade_agg
stg_history_trades >> int_trade_agg_week >> trade_agg
stg_history_trades >> int_trade_agg_year >> trade_agg
