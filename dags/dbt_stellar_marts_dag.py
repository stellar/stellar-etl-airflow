from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
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
    "dbt_stellar_marts",
    default_args=get_default_dag_args(),
    start_date=datetime(2025, 9, 7, 0, 0),
    description="This DAG runs dbt models at a daily cadence",
    schedule_interval="0 13 * * *",  # Runs at 13:00 UTC
    user_defined_filters={
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
    },
    max_active_runs=3,
    catchup=True,
    tags=["dbt-stellar-marts"],
    # sla_miss_callback=alert_sla_miss,
)

# Wait on ingestion DAGs
wait_on_dbt_enriched_base_tables = build_cross_deps(
    dag, "wait_on_dbt_enriched_base_tables", "dbt_enriched_base_tables"
)

# Wait on Snapshot DAGs which runs at 1:00 UTC (12 hours earlier)
wait_on_dbt_snapshot_tables = build_cross_deps(
    dag, "wait_on_dbt_snapshot_tables", "dbt_snapshot", time_delta=720
)

# Wait on Snapshot DAGs
wait_on_dbt_snapshot_pricing_tables = build_cross_deps(
    dag,
    "wait_on_dbt_snapshot_pricing_tables",
    "dbt_snapshot_pricing_data",
)

# DBT models to run
ohlc_task = dbt_task(dag, tag="ohlc", operator="+", excluded="stellar_dbt_public")

# This also creates the stg_history_trades view
trade_agg_task = dbt_task(dag, tag="trade_agg", operator="+")

fee_stats_agg_task = dbt_task(dag, tag="fee_stats")

asset_stats_agg_task = dbt_task(
    dag,
    tag="asset_stats",
    operator="+",
    excluded=["stellar_dbt_public", "+tag:enriched_history_operations"],
)

network_stats_agg_task = dbt_task(
    dag, tag="network_stats", excluded="stellar_dbt_public"
)

partnership_assets_task = dbt_task(
    dag,
    tag="partnership_assets",
    operator="+",
    excluded="stellar_dbt_public",
    date_macro="ds",
)

history_assets = dbt_task(dag, tag="history_assets", operator="+")

wallet_metrics_task = dbt_task(
    dag,
    tag="wallet_metrics",
    operator="+",
    excluded=["stellar_dbt_public", "+tag:entity_attribution"],
)

token_transfer_task = dbt_task(
    dag, tag="token_transfer", operator="+", excluded="stellar_dbt_public"
)

entity_attribution_task = dbt_task(
    dag, tag="entity_attribution", operator="+", excluded="stellar_dbt_public"
)

account_activity_task = dbt_task(
    dag,
    tag="account_activity",
    operator="+",
    excluded=[
        "stellar_dbt_public",
        "+tag:partnership_assets",
        "+tag:token_transfer",
        "+tag:entity_attribution",
        "+tag:wallet_metrics",
        "+tag:asset_prices",
    ],
)

tvl_task = dbt_task(dag, tag="tvl", operator="+", excluded="stellar_dbt_public")

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
    dag, tag="asset_balance_agg", operator="+", excluded="+snapshots"
)

asset_prices_task = dbt_task(dag, tag="asset_prices")

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
wait_on_dbt_enriched_base_tables >> network_stats_agg_task
wait_on_dbt_enriched_base_tables >> partnership_assets_task
wait_on_dbt_enriched_base_tables >> history_assets
wait_on_dbt_enriched_base_tables >> token_transfer_task
wait_on_dbt_enriched_base_tables >> entity_attribution_task >> wallet_metrics_task
wait_on_dbt_enriched_base_tables >> tvl_task >> export_tvl_to_gcs
wait_on_dbt_snapshot_tables >> asset_balance_agg_task
wait_on_dbt_snapshot_tables >> asset_prices_task >> account_activity_task
wait_on_dbt_snapshot_pricing_tables >> asset_prices_task
# wait_on_dbt_enriched_base_tables >> soroban
# wait_on_dbt_enriched_base_tables >> snapshot_state
# wait_on_dbt_enriched_base_tables >> relevant_asset_trades
