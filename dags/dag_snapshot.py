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
    "dbt_snapshot",
    default_args={**get_default_dag_args(), **{"depends_on_past": True}},
    start_date=datetime(2026, 1, 13, 13, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 1 * * *",  # Runs at 01:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["custom_snapshot"],
    params={
        "snapshot_start_date": Param(
            default="2026-01-01", type="string"
        ),  # only used for manual runs
        "snapshot_end_date": Param(
            default="2026-01-13", type="string"
        ),  # only used for manual runs
        "snapshot_full_refresh": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_trustline": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_accounts": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_liquidity_pools": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_evicted_keys": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_contract_data": Param(
            default="false", type="string"
        ),  # only used for manual runs
        "skip_reflector_prices_data": Param(
            default="false", type="string"
        ),  # only used for manual runs
    },
    # sla_miss_callback=alert_sla_miss,
)


def should_run_task(snapshot_name, **kwargs):
    return kwargs["params"].get(snapshot_name, "false") != "true"


wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables", time_delta=60
)

check_should_run_trustline = ShortCircuitOperator(
    task_id="check_should_run_trustline",
    python_callable=should_run_task,
    op_args=["skip_trustline"],
    provide_context=True,
    dag=dag,
)

check_should_run_accounts = ShortCircuitOperator(
    task_id="check_should_run_accounts",
    python_callable=should_run_task,
    op_args=["skip_accounts"],
    provide_context=True,
    dag=dag,
)

check_should_run_contract_data = ShortCircuitOperator(
    task_id="check_should_run_contract_data",
    python_callable=should_run_task,
    op_args=["skip_contract_data"],
    provide_context=True,
    dag=dag,
)

check_should_run_liquidity_pools = ShortCircuitOperator(
    task_id="check_should_run_liquidity_pools",
    python_callable=should_run_task,
    op_args=["skip_liquidity_pools"],
    provide_context=True,
    dag=dag,
)

check_should_run_evicted_keys = ShortCircuitOperator(
    task_id="check_should_run_evicted_keys",
    python_callable=should_run_task,
    op_args=["skip_evicted_keys"],
    provide_context=True,
    dag=dag,
)

check_should_run_reflector_prices_data = ShortCircuitOperator(
    task_id="check_should_run_reflector_prices_data",
    python_callable=should_run_task,
    op_args=["skip_reflector_prices_data"],
    provide_context=True,
    dag=dag,
)

trustline_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_trustline",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

accounts_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_accounts",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

liquidity_pools_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_liquidity_pools",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

evicted_keys_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_evicted_keys",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

contract_data_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_contract_data",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

reflector_prices_data_snapshot_task = dbt_task(
    dag,
    tag="custom_snapshot_reflector_prices_data",
    operator="+",
    excluded="+tag:custom_snapshot_contract_data",
    env_vars={
        "SNAPSHOT_START_DATE": "{{ ds if run_id.startswith('scheduled_') else params.snapshot_start_date }}",
        "SNAPSHOT_END_DATE": "{{ next_ds if run_id.startswith('scheduled_') else params.snapshot_end_date }}",
        "SNAPSHOT_FULL_REFRESH": "{{ false if run_id.startswith('scheduled_') else params.snapshot_full_refresh }}",
    },
)

(
    wait_on_dbt_enriched_base_tables
    >> check_should_run_trustline
    >> trustline_snapshot_task
)
(
    wait_on_dbt_enriched_base_tables
    >> check_should_run_accounts
    >> accounts_snapshot_task
)
(
    wait_on_dbt_enriched_base_tables
    >> check_should_run_liquidity_pools
    >> liquidity_pools_snapshot_task
)
(
    wait_on_dbt_enriched_base_tables
    >> check_should_run_evicted_keys
    >> evicted_keys_snapshot_task
)
(
    wait_on_dbt_enriched_base_tables
    >> check_should_run_contract_data
    >> contract_data_snapshot_task
)
(
    wait_on_dbt_enriched_base_tables
    >> check_should_run_reflector_prices_data
    >> contract_data_snapshot_task
    >> reflector_prices_data_snapshot_task
)
