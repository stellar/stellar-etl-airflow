from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Param, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow.default import get_default_dag_args, init_sentry

CLONE_SOURCES_VAR = "staging_clone_sources"
DEFAULT_LOCATION = "US"

ENVIRONMENT = Variable.get("sentry_environment", "production")
ALLOWED_ENVIRONMENTS = {"production", "staging"}
if ENVIRONMENT not in ALLOWED_ENVIRONMENTS:
    raise AirflowException(
        "dag_stellar_dbt_staging_refresh may only be parsed in production or staging environments."
    )

DISALLOWED_TARGET_PROJECTS = {"crypto-stellar", "hubble-261722"}

init_sentry()


def escape_sql_literal(value: str) -> str:
    """Escape single quotes so the value can be inlined in SQL strings."""
    if value is None:
        return ""
    return str(value).replace("'", "\\'")


def build_drop_staging_script(project: str, dataset: str, drop_flag: bool) -> str:
    """Return a BigQuery script that drops all tables/views in the staging dataset when enabled."""
    project = escape_sql_literal(project)
    dataset = escape_sql_literal(dataset)
    return f"""
DECLARE drop_enabled BOOL DEFAULT {'TRUE' if drop_flag else 'FALSE'};
IF drop_enabled THEN
FOR candidate IN (
    SELECT table_name, table_type
    FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES
) DO
    EXECUTE IMMEDIATE FORMAT(
    "DROP %s `%s.%s.%s`",
    IF(candidate.table_type = 'VIEW', 'VIEW', 'TABLE'),
    '{project}',
    '{dataset}',
    candidate.table_name
    );
END FOR;
END IF;
""".strip()


def build_schema_clone_script(
    sources: List[Dict[str, Any]], staging_project: str, staging_dataset: str
) -> str:
    """Build the SQL that clones tables and views from production datasets, supporting per-source targets."""
    entries = []
    for src in sources:
        project = escape_sql_literal(src.get("project"))
        dataset = escape_sql_literal(src.get("dataset"))
        if not project or not dataset:
            continue
        target_project = escape_sql_literal(
            src.get("target_project") or staging_project
        )
        target_dataset = escape_sql_literal(
            src.get("target_dataset") or staging_dataset
        )
        if target_project in DISALLOWED_TARGET_PROJECTS:
            raise AirflowException(
                "Schema-mode clones must write into staging datasets; modifications to crypto-stellar or hubble-261722 are not allowed."
            )
        entries.append(
            "STRUCT('"
            + project
            + "' AS project, '"
            + dataset
            + "' AS dataset, '"
            + target_project
            + "' AS target_project, '"
            + target_dataset
            + "' AS target_dataset)"
        )
    if not entries:
        raise AirflowException(
            "Schema clone requires at least one valid source dataset entry."
        )
    source_array = "[\n    " + ",\n    ".join(entries) + "\n]"
    staging_project = escape_sql_literal(staging_project)
    staging_dataset = escape_sql_literal(staging_dataset)
    table_query = (
        "SELECT ARRAY_AGG(table_name)\n"
        "FROM `%s.%s.INFORMATION_SCHEMA.TABLES`\n"
        "WHERE table_type = 'BASE TABLE'\n"
        "  AND NOT REGEXP_CONTAINS(table_name, r'_.*bkp_\\d{8}')"
    )
    view_query = (
        "SELECT ARRAY_AGG(table_name)\n"
        "FROM `%s.%s.INFORMATION_SCHEMA.VIEWS`\n"
        "WHERE NOT REGEXP_CONTAINS(table_name, r'_.*bkp_\\d{8}')"
    )

    return f"""
DECLARE staging_project STRING DEFAULT '{staging_project}';
DECLARE staging_dataset STRING DEFAULT '{staging_dataset}';
DECLARE source_datasets ARRAY<STRUCT<project STRING, dataset STRING, target_project STRING, target_dataset STRING>> = {source_array};

FOR source IN UNNEST(source_datasets) DO
    DECLARE table_names ARRAY<STRING>;
    EXECUTE IMMEDIATE FORMAT(
        '{table_query}',
        source.project,
        source.dataset
    ) INTO table_names;

  IF table_names IS NOT NULL THEN
    FOR table_name IN UNNEST(table_names) DO
      EXECUTE IMMEDIATE FORMAT(
        "CREATE OR REPLACE TABLE `%s.%s.%s` CLONE `%s.%s.%s`",
        source.target_project,
        source.target_dataset,
        table_name,
        source.project,
        source.dataset,
        table_name
      );
    END FOR;
  END IF;

  DECLARE view_names ARRAY<STRING>;
    EXECUTE IMMEDIATE FORMAT(
        '{view_query}',
        source.project,
        source.dataset
    ) INTO view_names;

  IF view_names IS NOT NULL THEN
    FOR view_name IN UNNEST(view_names) DO
      EXECUTE IMMEDIATE FORMAT(
        "CREATE OR REPLACE VIEW `%s.%s.%s` AS SELECT * FROM `%s.%s.%s`",
        source.target_project,
        source.target_dataset,
        view_name,
        source.project,
        source.dataset,
        view_name
      );
    END FOR;
  END IF;
END FOR;
""".strip()


def render_manual_array(values: List[str], struct_signature: str) -> str:
    """Format a list of STRUCT literals into a BigQuery array literal."""
    if not values:
        return f"CAST([] AS ARRAY<{struct_signature}>)"
    body = ",\n    ".join(values)
    return "[\n    " + body + "\n]"


def build_manual_clone_script(
    table_entries: List[Dict[str, Any]],
    view_entries: List[Dict[str, Any]],
    staging_project: str,
    staging_dataset: str,
) -> str:
    """Return SQL that clones the provided manual tables and views into staging, allowing per-entry targets."""
    staging_project = escape_sql_literal(staging_project)
    staging_dataset = escape_sql_literal(staging_dataset)

    table_structs = []
    for entry in table_entries:
        project = escape_sql_literal(entry.get("project"))
        dataset = escape_sql_literal(entry.get("dataset"))
        name = escape_sql_literal(entry.get("table"))
        if not project or not dataset or not name:
            continue
        target_project = escape_sql_literal(
            entry.get("target_project") or staging_project
        )
        target_dataset = escape_sql_literal(
            entry.get("target_dataset") or staging_dataset
        )
        if target_project in DISALLOWED_TARGET_PROJECTS:
            raise AirflowException(
                "Manual cloning must target staging datasets; dropping or cloning tables into crypto-stellar or hubble-261722 is forbidden."
            )
        table_structs.append(
            f"STRUCT('{project}' AS project, '{dataset}' AS dataset, '{name}' AS name, '{target_project}' AS target_project, '{target_dataset}' AS target_dataset)"
        )

    view_structs = []
    for entry in view_entries:
        project = escape_sql_literal(entry.get("project"))
        dataset = escape_sql_literal(entry.get("dataset"))
        name = escape_sql_literal(entry.get("view"))
        if not project or not dataset or not name:
            continue
        target_project = escape_sql_literal(
            entry.get("target_project") or staging_project
        )
        target_dataset = escape_sql_literal(
            entry.get("target_dataset") or staging_dataset
        )
        view_structs.append(
            f"STRUCT('{project}' AS project, '{dataset}' AS dataset, '{name}' AS name, '{target_project}' AS target_project, '{target_dataset}' AS target_dataset)"
        )

    tables_array = render_manual_array(
        table_structs,
        "STRUCT<project STRING, dataset STRING, name STRING, target_project STRING, target_dataset STRING>",
    )
    views_array = render_manual_array(
        view_structs,
        "STRUCT<project STRING, dataset STRING, name STRING, target_project STRING, target_dataset STRING>",
    )

    return f"""
DECLARE staging_project STRING DEFAULT '{staging_project}';
DECLARE staging_dataset STRING DEFAULT '{staging_dataset}';
DECLARE manual_tables ARRAY<STRUCT<project STRING, dataset STRING, name STRING, target_project STRING, target_dataset STRING>> = {tables_array};
DECLARE manual_views ARRAY<STRUCT<project STRING, dataset STRING, name STRING, target_project STRING, target_dataset STRING>> = {views_array};

IF ARRAY_LENGTH(manual_tables) > 0 THEN
  FOR entry IN UNNEST(manual_tables) DO
    EXECUTE IMMEDIATE FORMAT(
      "CREATE OR REPLACE TABLE `%s.%s.%s` CLONE `%s.%s.%s`",
      entry.target_project,
      entry.target_dataset,
      entry.name,
      entry.project,
      entry.dataset,
      entry.name
    );
  END FOR;
END IF;

IF ARRAY_LENGTH(manual_views) > 0 THEN
  FOR entry IN UNNEST(manual_views) DO
    EXECUTE IMMEDIATE FORMAT(
      "CREATE OR REPLACE VIEW `%s.%s.%s` AS SELECT * FROM `%s.%s.%s`",
      entry.target_project,
      entry.target_dataset,
      entry.name,
      entry.project,
      entry.dataset,
      entry.name
    );
  END FOR;
END IF;
""".strip()


def prepare_run_config(**context) -> Dict[str, str]:
    """Collect runtime parameters, validate them, and return drop/clone SQL."""
    conf = context["dag_run"].conf or {}
    params = context.get("params") or {}
    run_mode = conf.get("mode", params.get("mode", "schema"))
    staging_project = conf.get("target_project") or Variable.get("bq_project")
    staging_dataset = conf.get("target_dataset") or Variable.get("bq_dataset")
    if not staging_project or not staging_dataset:
        raise AirflowException(
            "Target project/dataset must be defined for the staging refresh."
        )

    drop_flag = conf.get(
        "drop_staging_objects", params.get("drop_staging_objects", True)
    )
    clone_sql = ""

    if run_mode == "schema":
        sources = conf.get("sources")
        if sources is None:
            sources = Variable.get(
                CLONE_SOURCES_VAR, default_var="[]", deserialize_json=True
            )
        if not isinstance(sources, list) or not sources:
            raise AirflowException(
                "Schema mode requires a list of source datasets. Set the staging_clone_sources variable or pass sources in dag_run.conf."
            )
        clone_sql = build_schema_clone_script(sources, staging_project, staging_dataset)
    else:
        tables = conf.get("tables", []) if run_mode in {"tables", "table"} else []
        views = conf.get("views", []) if run_mode in {"views", "view"} else []
        if run_mode == "table":
            tables = tables + [
                {
                    "project": conf.get("project"),
                    "dataset": conf.get("dataset"),
                    "table": conf.get("table"),
                }
            ]
        if run_mode == "view":
            views = views + [
                {
                    "project": conf.get("project"),
                    "dataset": conf.get("dataset"),
                    "view": conf.get("view"),
                }
            ]
        if not tables and not views:
            raise AirflowException(
                "Manual runs must supply at least one table or view definition in dag_run.conf."
            )
        clone_sql = build_manual_clone_script(
            tables, views, staging_project, staging_dataset
        )

    drop_sql = build_drop_staging_script(staging_project, staging_dataset, drop_flag)
    return {
        "drop_sql": drop_sql,
        "clone_sql": clone_sql,
    }


with DAG(
    dag_id="dag_stellar_dbt_staging_refresh",
    default_args=get_default_dag_args(),
    start_date=datetime(2026, 1, 20),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description=(
        "Staging refresh that clones production tables into the staging dataset only when manually triggered. "
        "To re-enable the weekly cadence, set `schedule_interval='0 10 * * 3'`."
    ),
    tags=["staging-refresh", "clone"],
    params={
        "mode": Param(
            default="schema",
            type="string",
            enum=["schema", "tables", "views", "table", "view"],
            description="Select schema-wide refresh or manual mode for tables/views.",
        ),
        "drop_staging_objects": Param(
            default=True,
            type="boolean",
            description="Whether to drop all staging tables/views before cloning (schema refresh).",
        ),
    },
) as dag:

    prep_config = PythonOperator(
        task_id="prepare_clone_config",
        python_callable=prepare_run_config,
        dag=dag,
    )

    drop_staging = BigQueryInsertJobOperator(
        task_id="drop_staging_objects",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='prepare_clone_config')['drop_sql'] }}",
                "useLegacySql": False,
            }
        },
        location=Variable.get("bq_location", default_var=DEFAULT_LOCATION),
    )

    clone_from_prod = BigQueryInsertJobOperator(
        task_id="clone_from_production",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='prepare_clone_config')['clone_sql'] }}",
                "useLegacySql": False,
            }
        },
        location=Variable.get("bq_location", default_var=DEFAULT_LOCATION),
    )

    prep_config >> drop_staging >> clone_from_prod
