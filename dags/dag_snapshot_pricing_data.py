from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import ShortCircuitOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import build_cross_deps
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

dag = DAG(
    "dbt_snapshot_pricing_data",
    default_args={**get_default_dag_args(), **{"depends_on_past": True}},
    start_date=datetime(2025, 10, 16, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 15 * * *",  # Runs at 15:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["custom_snapshot"],
    params={
        "snapshot_start_date": Param(
            default="2025-01-01", type="string"
        ),  # only used for manual runs
        "snapshot_end_date": Param(
            default="2025-01-02", type="string"
        ),  # only used for manual runs
        "snapshot_full_refresh": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_asset_prices_usd": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_wisdom_tree_asset_prices_data": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_euro_usd_ohlc": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_partnership_asset_prices": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_xlm_to_usd": Param(
            default="false", type="string"
        ),  # only used for manual runs
    },
    # sla_miss_callback=alert_sla_miss,
)


def should_run_task(snapshot_name, **kwargs):
    return kwargs["params"].get(snapshot_name, "false") != "true"


wait_on_external_data_dag_wisdom_tree_data = build_cross_deps(
    dag,
    "wait_on_external_data_dag_wisdom_tree_data",
    "external_data_dag",
    "del_ins_wisdom_tree_asset_prices_data_task",
    time_delta=120,
)


check_should_run_asset_prices_usd = ShortCircuitOperator(
    task_id="check_should_run_asset_prices_usd",
    python_callable=should_run_task,
    op_args=["skip_asset_prices_usd"],
    provide_context=True,
    dag=dag,
)

check_should_run_wisdom_tree_asset_prices_data = ShortCircuitOperator(
    task_id="check_should_run_wisdom_tree_asset_prices_data",
    python_callable=should_run_task,
    op_args=["skip_wisdom_tree_asset_prices_data"],
    provide_context=True,
    dag=dag,
)

check_should_run_euro_usd_ohlc = ShortCircuitOperator(
    task_id="check_should_run_euro_usd_ohlc",
    python_callable=should_run_task,
    op_args=["skip_euro_usd_ohlc"],
    provide_context=True,
    dag=dag,
)

check_should_run_partnership_asset_prices = ShortCircuitOperator(
    task_id="check_should_run_partnership_asset_prices",
    python_callable=should_run_task,
    op_args=["skip_partnership_asset_prices"],
    provide_context=True,
    dag=dag,
)

check_should_run_xlm_to_usd = ShortCircuitOperator(
    task_id="check_should_run_xlm_to_usd",
    python_callable=should_run_task,
    op_args=["skip_xlm_to_usd"],
    provide_context=True,
    dag=dag,
)

asset_prices_usd_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_asset_prices_usd",
    operator="+",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

wisdom_tree_asset_prices_data_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_wisdom_tree_asset_prices_data",
    operator="+",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

euro_usd_ohlc_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_euro_usd_ohlc",
    operator="+",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

partner_asset_prices_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_partnership_asset_prices",
    operator="+",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

xlm_to_usd_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_xlm_to_usd",
    operator="+",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

(
    wait_on_external_data_dag_wisdom_tree_data
    >> check_should_run_asset_prices_usd
    >> asset_prices_usd_snapshot_task
)
(
    wait_on_external_data_dag_wisdom_tree_data
    >> check_should_run_wisdom_tree_asset_prices_data
    >> wisdom_tree_asset_prices_data_snapshot_task
)
(
    wait_on_external_data_dag_wisdom_tree_data
    >> check_should_run_euro_usd_ohlc
    >> euro_usd_ohlc_snapshot_task
)
(
    wait_on_external_data_dag_wisdom_tree_data
    >> check_should_run_partnership_asset_prices
    >> partner_asset_prices_snapshot_task
)
(
    wait_on_external_data_dag_wisdom_tree_data
    >> check_should_run_xlm_to_usd
    >> xlm_to_usd_snapshot_task
)
