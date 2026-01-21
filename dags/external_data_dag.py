"""
The external_data_dag DAG exports data from external sources.
It is scheduled to export information to BigQuery at regular intervals.
"""

from ast import literal_eval
from datetime import datetime
from json import loads

from airflow import DAG
from airflow.configuration import conf
from airflow.models.variable import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from stellar_etl_airflow import macros
from stellar_etl_airflow.build_del_ins_operator import create_export_del_insert_operator
from stellar_etl_airflow.build_internal_export_task import build_export_task
from stellar_etl_airflow.default import get_default_dag_args, init_sentry
from stellar_etl_airflow.utils import access_secret

init_sentry()

EXTERNAL_DATA_TABLE_NAMES = Variable.get("table_ids", deserialize_json=True)
EXTERNAL_DATA_PROJECT_NAME = Variable.get("bq_project")
EXTERNAL_DATA_DATASET_NAME = Variable.get("bq_dataset")
DBT_TARGET_ENV = Variable.get("dbt_target")

RETOOL_TABLE_NAME = EXTERNAL_DATA_TABLE_NAMES["retool_entity_data"]
RETOOL_EXPORT_TASK_ID = "export_retool_data"

WISDOM_TREE_ASSET_PRICES_TABLE_NAME = EXTERNAL_DATA_TABLE_NAMES[
    "wisdom_tree_asset_prices_data"
]
WISDOM_TREE_ASSET_PRICES_EXPORT_TASK_ID = "export_wisdom_tree_asset_prices_data"

COINGECKO_PRICES_TABLE_NAME = EXTERNAL_DATA_TABLE_NAMES["asset_prices__coingecko"]
COINGECKO_PRICES_EXPORT_TASK_ID = "export_coingecko_prices_data"

DEFILLAMA_BORROWS_TABLE_NAME = EXTERNAL_DATA_TABLE_NAMES["defillama_borrows"]
DEFILLAMA_BORROWS_EXPORT_TASK_ID = "export_defillama_borrows"

DEFILLAMA_TVLS_TABLE_NAME = EXTERNAL_DATA_TABLE_NAMES["defillama_tvls"]
DEFILLAMA_TVLS_EXPORT_TASK_ID = "export_defillama_tvls"

# Initialize the DAG
dag = DAG(
    "external_data_dag",
    default_args=get_default_dag_args(),
    start_date=datetime(2026, 1, 13, 0, 0),
    description="This DAG exports data from external sources such as retool.",
    schedule_interval="0 13 * * *",
    params={
        "alias": "external",
        "manual_start_date": "",  # Format: YYYY-MM-DD HH:MM:SS or empty for scheduled
        "manual_end_date": "",  # Format: YYYY-MM-DD HH:MM:SS or empty for scheduled
    },
    render_template_as_native_obj=True,
    user_defined_macros={
        "subtract_data_interval": macros.subtract_data_interval,
        "batch_run_date_as_datetime_string": macros.batch_run_date_as_datetime_string,
    },
    user_defined_filters={
        "fromjson": lambda s: loads(s),
        "container_resources": lambda s: k8s.V1ResourceRequirements(requests=s),
        "literal_eval": lambda e: literal_eval(e),
    },
    catchup=True,
)


def extract_date_from_datetime(datetime_str):
    """Extracts the date (YYYY-MM-DD) from a UTC datetime string."""
    # Check if the input is an Airflow template string
    if "{{" in datetime_str and "}}" in datetime_str:
        return datetime_str  # Return the template string as-is for Airflow to resolve

    dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d")


# Ensure manual_start_date and manual_end_date are provided in ISO 8601 format (e.g., '2026-01-13T00:00:00Z').
# If not provided, defaults to data_interval_start and data_interval_end.
manual_start_date = (
    "{{ params.get('manual_start_date', data_interval_start.isoformat()) }}"
)
manual_end_date = "{{ params.get('manual_end_date', data_interval_end.isoformat()) }}"

# Convert datetime to date (YYYY-MM-DD) for further processing.
# This ensures compatibility with downstream tasks that may require only the date part.
start_date = extract_date_from_datetime(manual_start_date)
end_date = extract_date_from_datetime(manual_end_date)


# Build the export task for retool data.
# The --start-time and --end-time arguments must be in ISO 8601 format.
retool_export_task = build_export_task(
    dag,
    RETOOL_EXPORT_TASK_ID,
    command="export-retool",
    cmd_args=[
        "--start-time",
        "{{ params.get('manual_start_date') or subtract_data_interval(dag, data_interval_start).isoformat() }}",
        "--end-time",
        "{{ params.get('manual_end_date') or subtract_data_interval(dag, data_interval_end).isoformat() }}",
    ],
    use_gcs=True,
    env_vars={
        "RETOOL_API_KEY": access_secret("retool-api-key"),
    },
)


retool_insert_to_bq_task = create_export_del_insert_operator(
    dag,
    table_name=RETOOL_TABLE_NAME,
    project=EXTERNAL_DATA_PROJECT_NAME,
    dataset=EXTERNAL_DATA_DATASET_NAME,
    export_task_id=RETOOL_EXPORT_TASK_ID,
    source_object_suffix="",
    partition=False,
    cluster=False,
    table_id=f"{EXTERNAL_DATA_PROJECT_NAME}.{EXTERNAL_DATA_DATASET_NAME}.{RETOOL_TABLE_NAME}",
)

retool_export_task >> retool_insert_to_bq_task


wisdom_tree_asset_prices_export_task = build_export_task(
    dag,
    WISDOM_TREE_ASSET_PRICES_EXPORT_TASK_ID,
    command="export-wisdom-tree-asset-prices",
    cmd_args=[
        "--start-time",
        "{{ params.get('manual_end_date') or subtract_data_interval(dag, data_interval_end).isoformat() }}",
        "--end-time",
        "{{ params.get('manual_end_date') or subtract_data_interval(dag, data_interval_end).isoformat() }}",
    ],
    use_gcs=True,
    env_vars={
        "RWA_API_KEY": access_secret("rwa-api-key"),
    },
)


wisdom_tree_asset_prices_insert_to_bq_task = create_export_del_insert_operator(
    dag,
    table_name=WISDOM_TREE_ASSET_PRICES_TABLE_NAME,
    project=EXTERNAL_DATA_PROJECT_NAME,
    dataset=EXTERNAL_DATA_DATASET_NAME,
    export_task_id=WISDOM_TREE_ASSET_PRICES_EXPORT_TASK_ID,
    source_object_suffix="",
    partition=False,
    cluster=False,
    table_id=f"{EXTERNAL_DATA_PROJECT_NAME}.{EXTERNAL_DATA_DATASET_NAME}.{WISDOM_TREE_ASSET_PRICES_TABLE_NAME}",
)

wisdom_tree_asset_prices_export_task >> wisdom_tree_asset_prices_insert_to_bq_task


# Build the export task for Coingecko prices data.
# The --start-time and --end-time arguments must be in ISO 8601 format (e.g., '2026-01-13T00:00:00Z').
# If not provided, defaults to data_interval_start and data_interval_end.
coingecko_export_task = build_export_task(
    dag,
    COINGECKO_PRICES_EXPORT_TASK_ID,
    command="export-coingecko-prices",
    cmd_args=[
        "--start-time",
        "{{ params.get('manual_start_date') or subtract_data_interval(dag, data_interval_start).isoformat() }}",
        "--end-time",
        "{{ params.get('manual_end_date') or subtract_data_interval(dag, data_interval_end).isoformat() }}",
    ],
    use_gcs=True,
    env_vars={},
)


coingecko_prices_insert_to_bq_task = create_export_del_insert_operator(
    dag,
    table_name=COINGECKO_PRICES_TABLE_NAME,
    project=EXTERNAL_DATA_PROJECT_NAME,
    dataset=EXTERNAL_DATA_DATASET_NAME,
    export_task_id=COINGECKO_PRICES_EXPORT_TASK_ID,
    source_object_suffix="",
    partition=False,
    cluster=False,
    table_id=f"{EXTERNAL_DATA_PROJECT_NAME}.{EXTERNAL_DATA_DATASET_NAME}.{COINGECKO_PRICES_TABLE_NAME}",
)

coingecko_export_task >> coingecko_prices_insert_to_bq_task

defillama_borrows_export_task = build_export_task(
    dag,
    DEFILLAMA_BORROWS_EXPORT_TASK_ID,
    command="export-defillama-borrows",
    use_gcs=True,
    env_vars={
        "DEFILLAMA_API_KEY": access_secret("defillama-api-key"),
    },
)


defillama_borrows_insert_to_bq_task = create_export_del_insert_operator(
    dag,
    table_name=DEFILLAMA_BORROWS_TABLE_NAME,
    project=EXTERNAL_DATA_PROJECT_NAME,
    dataset=EXTERNAL_DATA_DATASET_NAME,
    export_task_id=DEFILLAMA_BORROWS_EXPORT_TASK_ID,
    source_object_suffix="",
    partition=False,
    cluster=False,
    table_id=f"{EXTERNAL_DATA_PROJECT_NAME}.{EXTERNAL_DATA_DATASET_NAME}.{DEFILLAMA_BORROWS_TABLE_NAME}",
)

defillama_borrows_export_task >> defillama_borrows_insert_to_bq_task

defillama_tvls_export_task = build_export_task(
    dag,
    DEFILLAMA_TVLS_EXPORT_TASK_ID,
    command="export-defillama-protocol-tvls",
    use_gcs=True,
    env_vars={
        "DEFILLAMA_API_KEY": access_secret("defillama-api-key"),
    },
)


defillama_tvls_insert_to_bq_task = create_export_del_insert_operator(
    dag,
    table_name=DEFILLAMA_TVLS_TABLE_NAME,
    project=EXTERNAL_DATA_PROJECT_NAME,
    dataset=EXTERNAL_DATA_DATASET_NAME,
    export_task_id=DEFILLAMA_TVLS_EXPORT_TASK_ID,
    source_object_suffix="",
    partition=False,
    cluster=False,
    table_id=f"{EXTERNAL_DATA_PROJECT_NAME}.{EXTERNAL_DATA_DATASET_NAME}.{DEFILLAMA_TVLS_TABLE_NAME}",
)

defillama_tvls_export_task >> defillama_tvls_insert_to_bq_task

stellar_expert_prices_export_task = build_export_task(
    dag,
    "export_stellar_expert_prices",
    command="export-stellar-expert-prices",
    cmd_args=[
        "--start-time",
        "{{ params.get('manual_start_date') or subtract_data_interval(dag, data_interval_start).isoformat() }}",
        "--end-time",
        "{{ params.get('manual_end_date') or subtract_data_interval(dag, data_interval_end).isoformat() }}",
        "--env",
        DBT_TARGET_ENV,
        "--asset-id",
        "{{ params.get('stellar_asset_id', '') }}",
        "--issuer",
        "{{ params.get('stellar_asset_issuer', '') }}",
        "--tier",
        "{{ params.get('stellar_asset_tier', 0) }}",
        "--truncate-table",
        "{{ params.get('stellar_truncate', false) | lower }}",
    ],
    env_vars={
        "STELLAR_EXPERT_SECRET_NAME": access_secret("stellar_expert_api_keys"),
    },
    use_gcs=False,
)
