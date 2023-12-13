import datetime

from airflow import DAG
from airflow.models.variable import Variable
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import build_dbt_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

init_sentry()

dag = DAG(
    "soroban_tables",
    default_args=get_default_dag_args(),
    start_date=datetime.datetime(2023, 9, 30),
    description="This DAG runs dbt to create the tables for the models in marts/soroban.",
    schedule_interval="0 17 * * *",  # Daily 11 AM UTC
    params={},
    catchup=True,
    max_active_runs=1,
)

# tasks for staging tables
stg_config_settings = build_dbt_task(dag, "stg_config_settings")
stg_contract_code = build_dbt_task(dag, "stg_contract_code")
stg_contract_data = build_dbt_task(dag, "stg_contract_data")
stg_ttl = build_dbt_task(dag, "stg_ttl")

# tasks for intermediate soroban aggregate tables
int_soroban__enriched_history_operations_year = build_dbt_task(
    dag, "int_soroban__enriched_history_operations_year"
)

int_soroban__contract_metrics_day = build_dbt_task(
    dag, "int_soroban__contract_metrics_day"
)
int_soroban__contract_metrics_day = build_dbt_task(
    dag, "int_soroban__contract_metrics_week"
)
int_soroban__contract_metrics_day = build_dbt_task(
    dag, "int_soroban__contract_metrics_month"
)
int_soroban__contract_metrics_day = build_dbt_task(
    dag, "int_soroban__contract_metrics_year"
)

int_soroban__top_contract_activity_day = build_dbt_task(
    dag, "int_soroban__top_contract_activity_day"
)
int_soroban__top_contract_activity_week = build_dbt_task(
    dag, "int_soroban__top_contract_activity_week"
)
int_soroban__top_contract_activity_month = build_dbt_task(
    dag, "int_soroban__top_contract_activity_month"
)
int_soroban__top_contract_activity_year = build_dbt_task(
    dag, "int_soroban__top_contract_activity_year"
)

# tasks for marts tables
contract_assets_fact = build_dbt_task(dag, "contract_assets_fact")
contract_type_fact = build_dbt_task(dag, "contract_type_fact")
ttl_current = build_dbt_task(dag, "ttl_current")
soroban__contract_metrics_agg = build_dbt_task(dag, "soroban__contract_metrics_agg")
soroban__top_contract_activity_agg = build_dbt_task(
    dag, "soroban__top_contract_activity_agg"
)

# DAG task graph
stg_config_settings
stg_contract_code

stg_contract_data >> contract_assets_fact
contract_type_fact
stg_ttl >> ttl_current

(
    int_soroban__enriched_history_operations_year
    >> int_soroban__contract_metrics_day
    >> soroban__contract_metrics_agg
)
(
    int_soroban__enriched_history_operations_year
    >> int_soroban__contract_metrics_day
    >> soroban__contract_metrics_agg
)
(
    int_soroban__enriched_history_operations_year
    >> int_soroban__contract_metrics_day
    >> soroban__contract_metrics_agg
)
(
    int_soroban__enriched_history_operations_year
    >> int_soroban__contract_metrics_day
    >> soroban__contract_metrics_agg
)

(
    int_soroban__enriched_history_operations_year
    >> int_soroban__top_contract_activity_day
    >> soroban__top_contract_activity_agg
)
(
    int_soroban__enriched_history_operations_year
    >> int_soroban__top_contract_activity_week
    >> soroban__top_contract_activity_agg
)
(
    int_soroban__enriched_history_operations_year
    >> int_soroban__top_contract_activity_month
    >> soroban__top_contract_activity_agg
)
(
    int_soroban__enriched_history_operations_year
    >> int_soroban__top_contract_activity_year
    >> soroban__top_contract_activity_agg
)
