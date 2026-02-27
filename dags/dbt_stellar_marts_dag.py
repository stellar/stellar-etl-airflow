from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s
from stellar_etl_airflow.build_cross_dependency_task import (
    LatestDagRunSensor,
    build_cross_deps,
)
from stellar_etl_airflow.build_dbt_task import dbt_task
from stellar_etl_airflow.default import (
    alert_sla_miss,
    get_default_dag_args,
    init_sentry,
)

init_sentry()

dag = DAG(
    "dbt_stellar_marts",
    default_args=get_default_dag_args(),
    start_date=datetime(2026, 1, 14, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 13 * * *",  # Runs at 13:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=1,
    catchup=True,
    tags=["dbt-stellar-marts"],
    # sla_miss_callback=alert_sla_miss,
)

batch_start_date = "{{ dag_run.conf.get('batch_start_date', data_interval_start) }}"
batch_end_date = "{{ dag_run.conf.get('batch_end_date', data_interval_end) }}"

# Wait on ingestion DAGs
wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables"
)

# Wait on Snapshot DAGs which runs at 1:00 UTC (12 hours earlier)
wait_on_dbt_snapshot_tables = build_cross_deps(
    dag, "wait_on_dbt_snapshot_tables", "dbt_snapshot", time_delta=720
)

# Wait on Snapshot DAGs
wait_on_dbt_wisdom_tree_snapshot_pricing_tables = build_cross_deps(
    dag,
    "wait_on_dbt_wisdom_tree_snapshot_pricing_tables",
    "dbt_snapshot_pricing_data",
    "dbt_build_custom_snapshot_wisdom_tree_asset_prices_data",
)

wait_on_dbt_partnership_assets_snapshot_pricing_tables = build_cross_deps(
    dag,
    "wait_on_dbt_partnership_assets_snapshot_pricing_tables",
    "dbt_snapshot_pricing_data",
    "dbt_build_custom_snapshot_partnership_asset_prices",
)

wait_on_dbt_coingecko_snapshot_pricing_tables = build_cross_deps(
    dag,
    "wait_on_dbt_coingecko_snapshot_pricing_tables",
    "dbt_snapshot_pricing_data",
    "dbt_build_custom_snapshot_asset_prices_coingecko",
)


any_pricing_data_available = EmptyOperator(
    dag=dag,
    task_id="any_pricing_data_available",
    trigger_rule=TriggerRule.ONE_SUCCESS,
)

[
    wait_on_dbt_wisdom_tree_snapshot_pricing_tables,
    wait_on_dbt_partnership_assets_snapshot_pricing_tables,
    wait_on_dbt_coingecko_snapshot_pricing_tables,
] >> any_pricing_data_available

# Wait on partner pipeline DAG
# Hardcoding timeout for this wait task to 2.5 hours to tolerate
# occasional lag in partner file delivery around 15:10 UTC.
wait_on_partner_pipeline = LatestDagRunSensor(
    task_id="check_wait_on_partner_pipeline_finish",
    external_dag_id="partner_pipeline_dag",
    min_execution_date_fn=lambda context: context["data_interval_start"],
    timeout=9000,
    dag=dag,
)

# DBT models to run
ohlc_task = dbt_task(dag, tag="ohlc", operator="+", excluded="stellar_dbt_public")

# This also creates the stg_history_trades view
trade_agg_task = dbt_task(dag, tag="trade_agg", operator="+")

fee_stats_agg_task = dbt_task(dag, tag="fee_stats")

asset_stats_agg_task = dbt_task(
    dag,
    tag="asset_stats",
    excluded=["stellar_dbt_public", "+tag:enriched_history_operations"],
)

network_stats_agg_task = dbt_task(
    dag, tag="network_stats", excluded="stellar_dbt_public"
)

# partnership_assets_task = dbt_task(
#     dag,
#     tag="partnership_assets",
#     operator="+",
#     excluded="stellar_dbt_public",
#     date_macro="ds",
# )

history_assets = dbt_task(dag, tag="history_assets", operator="+")

wallet_metrics_task = dbt_task(
    dag,
    tag="wallet_metrics",
    operator="+",
    excluded=["stellar_dbt_public", "+tag:entity_attribution"],
)

entity_attribution_task = dbt_task(
    dag,
    tag="entity_attribution",
    operator="+",
    excluded=[
        "stellar_dbt_public",
        "+tag:token_transfer",
    ],
    batch_start_date=batch_start_date,
    batch_end_date=batch_end_date,
)

# TODO: account_activity currently runs as part of entity_attribution
# account_activity_task = dbt_task(
#     dag,
#     tag="account_activity",
#     operator="+",
#     excluded=[
#         "stellar_dbt_public",
#         "+tag:partnership_assets",
#         "+tag:token_transfer",
#         "+tag:entity_attribution",
#         "+tag:wallet_metrics",
#         "+tag:asset_prices",
#     ],
# )

tvl_task = dbt_task(dag, tag="tvl", excluded="stellar_dbt_public")

project = "{{ var.value.bq_project }}"
dataset = "{{ var.value.dbt_internal_marts_dataset }}"
defillama_tvl_bucket_name = Variable.get("defillama_tvl_bucket_name")
gcs_uri = (
    "{% raw %}gs://{% endraw %}"
    + f"{defillama_tvl_bucket_name}"
    + "{% raw %}/stellar-tvl.json{% endraw %}"
)

export_tvl_to_gcs = BigQueryInsertJobOperator(
    task_id="export_tvl_to_gcs",
    configuration={
        "extract": {
            "sourceTable": {
                "projectId": project,
                "datasetId": dataset,
                "tableId": "tvl_agg",
            },
            "destinationUris": [gcs_uri],
            "compression": "NONE",
            "destinationFormat": "NEWLINE_DELIMITED_JSON",
            "printHeader": False,
        }
    },
    location="US",
)

asset_balance_agg_task = dbt_task(
    dag,
    tag="asset_balance_agg",
    operator="+",
    excluded=[
        "+snapshots",
        "assets",
        "+tag:token_transfer",
        "+tag:entity_attribution",
        "+tag:wallet_metrics",
    ],
)

# asset_prices runs in two phases:
#   1. dbt build --target staging  → writes to sdf_marts_staging, tests run there
#   2. dbt run  --target prod      → writes to sdf_marts only if staging tests passed
asset_prices_staging = dbt_task(dag, tag="asset_prices",
                                 command_type="build", target="staging")
asset_prices_prod = dbt_task(dag, tag="asset_prices", command_type="run")
asset_prices_staging >> asset_prices_prod

assets_task = dbt_task(dag, tag="assets")

omni_pdt_agg_task = dbt_task(dag, tag="omni_pdts")

stellarbeat_task = dbt_task(dag, tag="stellarbeat")

# Disable soroban tables because they're broken
# soroban = dbt_task(dag, tag="soroban", operator="+")
# Disable snapshot state tables because they're broken
# snapshot_state = dbt_task(dag, tag="snapshot_state")
# Disable releveant_asset_trades due to bugs in SCD tables
# relevant_asset_trades = dbt_task(dag, tag="relevant_asset_trades")

# DAG task graph
wait_on_dbt_enriched_base_tables >> trade_agg_task
wait_on_dbt_enriched_base_tables >> fee_stats_agg_task
wait_on_dbt_enriched_base_tables >> asset_stats_agg_task
asset_prices_prod >> asset_stats_agg_task
wait_on_dbt_enriched_base_tables >> network_stats_agg_task
# wait_on_dbt_enriched_base_tables >> partnership_assets_task
wait_on_dbt_enriched_base_tables >> history_assets
wait_on_partner_pipeline >> entity_attribution_task
wait_on_dbt_enriched_base_tables >> entity_attribution_task >> wallet_metrics_task
wait_on_dbt_enriched_base_tables >> tvl_task >> export_tvl_to_gcs
asset_prices_prod >> tvl_task
wait_on_dbt_snapshot_tables >> asset_balance_agg_task
any_pricing_data_available >> asset_prices_staging

# account_activity is built as part of the entity_attribution while we refactor
# Because of this we need token_transfers to run before entity_attribution
# so that account_activity will work as intended until we refactor
# asset_prices_prod >> account_activity_task
# partnership_assets_task >> account_activity_task
# wallet_metrics_task >> account_activity_task

entity_attribution_task >> assets_task
entity_attribution_task >> omni_pdt_agg_task
asset_balance_agg_task >> omni_pdt_agg_task
asset_prices_prod >> omni_pdt_agg_task

# wait_on_dbt_enriched_base_tables >> soroban
# wait_on_dbt_enriched_base_tables >> snapshot_state
# wait_on_dbt_enriched_base_tables >> relevant_asset_trades

stellarbeat_task
