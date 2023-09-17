import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.build_delete_data_task import build_delete_dbt_data_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "asset_pricing",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 9, 10),
    description="This DAG runs dbt to calculate asset pricing based on stablecoin and XLM trades",
    schedule_interval="0 2 * * *",  # daily at 2am
    catchup=True,
    params={},
    user_defined_filters={"fromjson": lambda s: json.loads(s)},
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
)

internal_project = Variable.get("bq_project")
dbt_conformed = Variable.get("sdf_conformed")
dbt_marts = Variable.get("sdf_marts")

# tasks for staging tables for trades
stg_history_trades = build_dbt_task(dag, "stg_history_trades")
stg_asset_prices_usd = build_dbt_task(dag, "stg_asset_prices_usd")

# tasks for deleting old data in case of reruns
delete_int_usdc_trades = build_delete_dbt_data_task(
    dag, internal_project, dbt_conformed, "int_usdc_trades", "closed_at"
)

delete_int_usds_trades = build_delete_dbt_data_task(
    dag, internal_project, dbt_conformed, "int_usds_trades", "closed_at"
)

delete_int_xlm_trades = build_delete_dbt_data_task(
    dag, internal_project, dbt_conformed, "int_xlm_trades", "closed_at"
)

delete_int_stable_coin_prices = build_delete_dbt_data_task(
    dag, internal_project, dbt_conformed, "int_stable_coin_prices", "closed_date"
)

delete_ohlc_exchange_fact = build_delete_dbt_data_task(
    dag, internal_project, dbt_marts, "ohlc_exchange_fact", "closed_date"
)

# tasks for ohlc intermediate tables
int_stable_coin_prices = build_dbt_task(dag, "int_stable_coin_prices")
int_usdc_trades = build_dbt_task(dag, "int_usdc_trades")
int_usds_trades = build_dbt_task(dag, "int_usds_trades")
int_xlm_trades = build_dbt_task(dag, "int_xlm_trades")

# tasks for final fact
ohlc_exchange_fact = build_dbt_task(dag, "ohlc_exchange_fact")

# DAG task graph
stg_history_trades >> delete_int_usdc_trades >> int_usdc_trades >> delete_ohlc_exchange_fact
stg_history_trades >> delete_int_usds_trades >> int_usds_trades >> delete_ohlc_exchange_fact
stg_history_trades >> delete_int_xlm_trades >> int_xlm_trades >> delete_ohlc_exchange_fact
int_xlm_trades >> delete_int_stable_coin_prices >> int_stable_coin_prices >> delete_ohlc_exchange_fact

stg_asset_prices_usd >> delete_ohlc_exchange_fact
delete_ohlc_exchange_fact >> ohlc_exchange_fact
