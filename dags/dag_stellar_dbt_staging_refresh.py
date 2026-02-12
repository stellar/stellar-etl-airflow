import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Param, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from stellar_etl_airflow.default import (
    get_default_dag_args,
    init_sentry,
    log_dag_failure,
    log_dag_success,
)

CLONE_SOURCES_VAR = "staging_clone_sources"
DEFAULT_LOCATION = "US"

ENVIRONMENT = Variable.get("sentry_environment", "production")
ALLOWED_ENVIRONMENTS = {"production", "staging"}
logger = logging.getLogger(__name__)

DISALLOWED_TARGET_PROJECTS = {"crypto-stellar", "hubble-261722"}


def escape_sql_literal(value: str) -> str:
    """Escape single quotes so the value can be inlined in SQL strings."""
    if value is None:
        return ""
    return str(value).replace("'", "''")


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
    sources: List[Dict[str, Any]],
    staging_project: str,
    staging_dataset: str,
    time_travel_hours: Optional[int] = None,
) -> str:
    """Build SQL to clone tables/views from production datasets, supporting per-source targets."""
    scripts: List[str] = []

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

        clone_suffix = (
            f" FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(time_travel_hours)} HOUR)"
            if time_travel_hours is not None
            else ""
        )

        block = f"""
BEGIN
DECLARE missing_tables ARRAY<STRING> DEFAULT [];
DECLARE missing_views ARRAY<STRING> DEFAULT [];
DECLARE missing_datasets ARRAY<STRING> DEFAULT [];
DECLARE cloned_tables ARRAY<STRING> DEFAULT [];
DECLARE cloned_views ARRAY<STRING> DEFAULT [];
DECLARE objects ARRAY<STRUCT<name STRING, is_view BOOL>>;

DECLARE dataset_exists BOOL DEFAULT FALSE;
SET dataset_exists = EXISTS (
    SELECT 1 FROM `{project}.INFORMATION_SCHEMA.SCHEMATA` WHERE schema_name = '{dataset}'
);

IF NOT dataset_exists THEN
    SET missing_datasets = ARRAY_CONCAT(missing_datasets, ['{project}.{dataset}']);
ELSE
    SET objects = ARRAY(
        SELECT AS STRUCT table_name AS name, FALSE AS is_view
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
            AND NOT REGEXP_CONTAINS(table_name, '_.*bkp_[0-9]{{8}}')
        UNION ALL
        SELECT AS STRUCT table_name AS name, TRUE AS is_view
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
        WHERE NOT REGEXP_CONTAINS(table_name, '_.*bkp_[0-9]{{8}}')
    );

    FOR obj IN (
        SELECT AS STRUCT *
        FROM UNNEST(objects)
    ) DO
        IF obj.is_view THEN
            IF EXISTS (
                SELECT 1
                FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`
                WHERE table_name = obj.name
            ) THEN
                BEGIN
                    EXECUTE IMMEDIATE FORMAT(
                        "CREATE OR REPLACE VIEW `%s.%s.%s` AS SELECT * FROM `%s.%s.%s`",
                        '{target_project}', '{target_dataset}', obj.name,
                        '{project}', '{dataset}', obj.name
                    );
                    SET cloned_views = ARRAY_CONCAT(cloned_views, [obj.name]);
                EXCEPTION WHEN ERROR THEN
                    SET missing_views = ARRAY_CONCAT(missing_views, [obj.name]);
                END;
            ELSE
                SET missing_views = ARRAY_CONCAT(missing_views, [obj.name]);
            END IF;
        ELSE
            IF EXISTS (
                SELECT 1
                FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name = obj.name
            ) THEN
                BEGIN
                    EXECUTE IMMEDIATE FORMAT(
                        "CREATE OR REPLACE TABLE `%s.%s.%s` CLONE `%s.%s.%s`{clone_suffix}",
                        '{target_project}', '{target_dataset}', obj.name,
                        '{project}', '{dataset}', obj.name
                    );
                    SET cloned_tables = ARRAY_CONCAT(cloned_tables, [obj.name]);
                EXCEPTION WHEN ERROR THEN
                    SET missing_tables = ARRAY_CONCAT(missing_tables, [obj.name]);
                END;
            ELSE
                SET missing_tables = ARRAY_CONCAT(missing_tables, [obj.name]);
            END IF;
        END IF;
    END FOR;
END IF;

SELECT 'cloned_table' AS kind, t AS name FROM UNNEST(cloned_tables) AS t
UNION ALL
SELECT 'cloned_view' AS kind, v AS name FROM UNNEST(cloned_views) AS v
UNION ALL
SELECT 'missing_table' AS kind, mt AS name FROM UNNEST(missing_tables) AS mt
UNION ALL
SELECT 'missing_view' AS kind, mv AS name FROM UNNEST(missing_views) AS mv
UNION ALL
SELECT 'missing_dataset' AS kind, d AS name FROM UNNEST(missing_datasets) AS d;
END;
""".strip()

    scripts.append(block)

    if not scripts:
        raise AirflowException(
            "Schema clone requires at least one valid source dataset entry."
        )

    return "\n\n".join(scripts)


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
    time_travel_hours: Optional[int] = None,
) -> str:
    """Return SQL that clones the provided manual tables and views into staging, allowing per-entry targets."""
    staging_project = escape_sql_literal(staging_project)
    staging_dataset = escape_sql_literal(staging_dataset)
    clone_suffix = (
        f" FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(time_travel_hours)} HOUR)"
        if time_travel_hours is not None
        else ""
    )

    table_structs: List[str] = []
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

    view_structs: List[str] = []
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
        if target_project in DISALLOWED_TARGET_PROJECTS:
            raise AirflowException(
                "Manual cloning must target staging datasets; dropping or cloning tables into crypto-stellar or hubble-261722 is forbidden."
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

    block = f"""
BEGIN
DECLARE staging_project STRING DEFAULT '{staging_project}';
DECLARE staging_dataset STRING DEFAULT '{staging_dataset}';
DECLARE missing_tables ARRAY<STRING> DEFAULT [];
DECLARE missing_views ARRAY<STRING> DEFAULT [];
DECLARE missing_datasets ARRAY<STRING> DEFAULT [];
DECLARE cloned_tables ARRAY<STRING> DEFAULT [];
DECLARE cloned_views ARRAY<STRING> DEFAULT [];
DECLARE manual_tables ARRAY<STRUCT<project STRING, dataset STRING, name STRING, target_project STRING, target_dataset STRING>> DEFAULT {tables_array};
DECLARE manual_views ARRAY<STRUCT<project STRING, dataset STRING, name STRING, target_project STRING, target_dataset STRING>> DEFAULT {views_array};
DECLARE src_dataset_exists BOOL DEFAULT FALSE;

IF ARRAY_LENGTH(manual_tables) > 0 THEN
    FOR entry IN (
        SELECT AS STRUCT *
        FROM UNNEST(manual_tables)
    ) DO
        SET src_dataset_exists = FALSE;
        BEGIN
            EXECUTE IMMEDIATE FORMAT(
                "SELECT COUNT(1)>0 FROM `%s`.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name=@ds",
                entry.project
            ) INTO src_dataset_exists USING entry.dataset AS ds;
        EXCEPTION WHEN ERROR THEN
            SET src_dataset_exists = FALSE;
        END;

        IF NOT src_dataset_exists THEN
            SET missing_datasets = ARRAY_CONCAT(missing_datasets, [FORMAT('%s.%s', entry.project, entry.dataset)]);
            CONTINUE;
        END IF;

        BEGIN
            EXECUTE IMMEDIATE FORMAT(
                "CREATE OR REPLACE TABLE `%s.%s.%s` CLONE `%s.%s.%s`{clone_suffix}",
                entry.target_project,
                entry.target_dataset,
                entry.name,
                entry.project,
                entry.dataset,
                entry.name
            );
            SET cloned_tables = ARRAY_CONCAT(cloned_tables, [entry.name]);
        EXCEPTION WHEN ERROR THEN
            SET missing_tables = ARRAY_CONCAT(missing_tables, [entry.name]);
        END;
    END FOR;
END IF;

IF ARRAY_LENGTH(manual_views) > 0 THEN
    FOR entry IN (
        SELECT AS STRUCT *
        FROM UNNEST(manual_views)
    ) DO
        SET src_dataset_exists = FALSE;
        BEGIN
            EXECUTE IMMEDIATE FORMAT(
                "SELECT COUNT(1)>0 FROM `%s`.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name=@ds",
                entry.project
            ) INTO src_dataset_exists USING entry.dataset AS ds;
        EXCEPTION WHEN ERROR THEN
            SET src_dataset_exists = FALSE;
        END;

        IF NOT src_dataset_exists THEN
            SET missing_datasets = ARRAY_CONCAT(missing_datasets, [FORMAT('%s.%s', entry.project, entry.dataset)]);
            CONTINUE;
        END IF;

        BEGIN
            EXECUTE IMMEDIATE FORMAT(
                "CREATE OR REPLACE VIEW `%s.%s.%s` AS SELECT * FROM `%s.%s.%s`",
                entry.target_project,
                entry.target_dataset,
                entry.name,
                entry.project,
                entry.dataset,
                entry.name
            );
            SET cloned_views = ARRAY_CONCAT(cloned_views, [entry.name]);
        EXCEPTION WHEN ERROR THEN
            SET missing_views = ARRAY_CONCAT(missing_views, [entry.name]);
        END;
    END FOR;
END IF;

SELECT 'cloned_table' AS kind, t AS name FROM UNNEST(cloned_tables) AS t
UNION ALL
SELECT 'cloned_view' AS kind, v AS name FROM UNNEST(cloned_views) AS v
UNION ALL
SELECT 'missing_table' AS kind, mt AS name FROM UNNEST(missing_tables) AS mt
UNION ALL
SELECT 'missing_view' AS kind, mv AS name FROM UNNEST(missing_views) AS mv
UNION ALL
SELECT 'missing_dataset' AS kind, d AS name FROM UNNEST(missing_datasets) AS d;
END;
""".strip()

    return block


def prepare_run_config(**context) -> Dict[str, Any]:
    """Collect runtime parameters and build per-source drop/clone job configs."""
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
    time_travel_hours_raw = conf.get("time_travel_hours") or params.get(
        "time_travel_hours"
    )
    time_travel_hours = None
    if time_travel_hours_raw is not None:
        try:
            time_travel_hours = int(time_travel_hours_raw)
            if time_travel_hours < 0:
                raise ValueError
        except (TypeError, ValueError):
            raise AirflowException(
                "time_travel_hours must be a non-negative integer (hours back from now)."
            )

    drop_jobs: List[Dict[str, Any]] = []
    clone_jobs: List[Dict[str, Any]] = []

    if run_mode in {"schema", "datasets"}:
        sources = conf.get("sources") if run_mode == "schema" else None
        if sources is None:
            sources = Variable.get(
                CLONE_SOURCES_VAR, default_var="[]", deserialize_json=True
            )
        if run_mode == "datasets":
            datasets_conf = conf.get("datasets")
            if datasets_conf:
                if not isinstance(datasets_conf, list):
                    raise AirflowException(
                        "datasets mode expects 'datasets' to be a list of project/dataset objects."
                    )
                sources = []
                for ds in datasets_conf:
                    project = ds.get("project")
                    dataset = ds.get("dataset")
                    if not project or not dataset:
                        raise AirflowException(
                            "Each entry in datasets must include project and dataset."
                        )
                    sources.append(
                        {
                            "project": project,
                            "dataset": dataset,
                            "target_project": ds.get("target_project"),
                            "target_dataset": ds.get("target_dataset"),
                        }
                    )
            else:
                project = conf.get("project")
                dataset = conf.get("dataset")
                if not project or not dataset:
                    raise AirflowException(
                        "datasets mode requires project/dataset or a datasets list in dag_run.conf."
                    )
                sources = [
                    {
                        "project": project,
                        "dataset": dataset,
                        "target_project": conf.get("target_project"),
                        "target_dataset": conf.get("target_dataset"),
                    }
                ]
        if not isinstance(sources, list) or not sources:
            raise AirflowException(
                "Schema mode requires a list of source datasets. Set the staging_clone_sources variable or pass sources in dag_run.conf."
            )

        # Build one drop/clone job per source to keep BigQuery jobs smaller.
        seen_targets = set()
        for src in sources:
            target_project = src.get("target_project") or staging_project
            target_dataset = src.get("target_dataset") or staging_dataset
            clone_sql = build_schema_clone_script(
                [src],
                target_project,
                target_dataset,
                time_travel_hours=time_travel_hours,
            )
            clone_jobs.append(
                {
                    "query": {
                        "query": clone_sql,
                        "useLegacySql": False,
                    }
                }
            )

            target_key = (target_project, target_dataset)
            if target_key not in seen_targets:
                seen_targets.add(target_key)
                drop_sql = build_drop_staging_script(
                    target_project, target_dataset, drop_flag
                )
                drop_jobs.append(
                    {
                        "query": {
                            "query": drop_sql,
                            "useLegacySql": False,
                        }
                    }
                )

    else:
        if run_mode != "tables_views":
            raise AirflowException(
                "Manual runs now use mode='tables_views' with tables/views arrays (or single table/view convenience fields)."
            )
        tables = conf.get("tables", [])
        views = conf.get("views", [])
        # Allow convenience of a single table/view passed alongside tables/views lists.
        if conf.get("table"):
            tables = tables + [
                {
                    "project": conf.get("project"),
                    "dataset": conf.get("dataset"),
                    "table": conf.get("table"),
                }
            ]
        if conf.get("view"):
            views = views + [
                {
                    "project": conf.get("project"),
                    "dataset": conf.get("dataset"),
                    "view": conf.get("view"),
                }
            ]
        if not tables and not views:
            raise AirflowException(
                "Manual runs must supply at least one table or view definition in dag_run.conf (tables/views arrays or a single table/view)."
            )
        clone_sql = build_manual_clone_script(
            tables,
            views,
            staging_project,
            staging_dataset,
            time_travel_hours=time_travel_hours,
        )
        clone_jobs.append(
            {
                "query": {
                    "query": clone_sql,
                    "useLegacySql": False,
                }
            }
        )
        drop_sql = build_drop_staging_script(
            staging_project, staging_dataset, drop_flag
        )
        drop_jobs.append(
            {
                "query": {
                    "query": drop_sql,
                    "useLegacySql": False,
                }
            }
        )

    return {
        "drop_jobs": drop_jobs,
        "clone_jobs": clone_jobs,
    }


def extract_from_config(key: str, **context) -> List[Dict[str, Any]]:
    """Pull a list from prepare_clone_config output without using custom XCom keys in mapping."""
    config = context["ti"].xcom_pull(task_ids="prepare_clone_config") or {}
    return config.get(key, [])


def log_job_results(
    job_id: str, project: Optional[str] = None, location: str = DEFAULT_LOCATION
) -> None:
    """Fetch query results for a completed BigQuery job and log successes/failures."""
    project = project or Variable.get("bq_project")
    hook = BigQueryHook()
    rows = list(
        hook.get_query_results(
            job_id=job_id,
            project_id=project,
            location=location,
        )
    )
    logger.info("BigQuery job %s returned %d rows", job_id, len(rows))
    for row in rows:
        # row is a dict-like object with keys matching column names
        logger.info(
            "job %s result: kind=%s name=%s", job_id, row.get("kind"), row.get("name")
        )


def log_all_job_results(
    job_ids: Any, project: Optional[str] = None, location: str = DEFAULT_LOCATION
) -> None:
    """Log results for one or many job_ids; accepts a single string or list/tuple."""
    if job_ids is None:
        logger.info("No job ids to log")
        return
    if isinstance(job_ids, str):
        ids = [job_ids]
    else:
        ids = list(job_ids)
    for job_id in ids:
        try:
            log_job_results(job_id, project=project, location=location)
        except Exception:
            logger.exception("Failed to log results for job %s", job_id)


def log_job_results_task(job_id: Any, *args: Any, **kwargs: Any) -> None:
    """Wrapper for mapped PythonOperator to log a single job id."""
    if isinstance(job_id, dict) and "job_id" in job_id:
        job_id = job_id.get("job_id")
    # Support LazySelectSequence or list of job ids from mapped BigQueryInsertJobOperator
    log_all_job_results(job_id)


if ENVIRONMENT not in ALLOWED_ENVIRONMENTS:
    logger.warning(
        "Skipping dag_stellar_dbt_staging_refresh creation; environment '%s' not in %s.",
        ENVIRONMENT,
        ALLOWED_ENVIRONMENTS,
    )
    dag = None
else:
    init_sentry()

    with DAG(
        dag_id="dag_stellar_dbt_staging_refresh",
        default_args=get_default_dag_args(),
        start_date=datetime(2026, 1, 20),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        description=(
            "Staging refresh that clones production tables into the staging dataset only when manually triggered. "
            "To re-enable the weekly cadence, set schedule='0 10 * * 3'."
        ),
        tags=["staging-refresh", "clone"],
        on_success_callback=log_dag_success,
        on_failure_callback=log_dag_failure,
        params={
            "mode": Param(
                default="schema",
                type="string",
                enum=["schema", "datasets", "tables_views"],
                description="Select schema-wide refresh, datasets mode, or manual tables/views together (tables_views).",
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

        drop_config = PythonOperator(
            task_id="build_drop_jobs",
            python_callable=lambda **context: extract_from_config(
                "drop_jobs", **context
            ),
            dag=dag,
        )

        clone_config = PythonOperator(
            task_id="build_clone_jobs",
            python_callable=lambda **context: extract_from_config(
                "clone_jobs", **context
            ),
            dag=dag,
        )

        drop_jobs = BigQueryInsertJobOperator.partial(
            task_id="drop_staging_objects",
            location=DEFAULT_LOCATION,
        ).expand(configuration=drop_config.output)

        clone_jobs = BigQueryInsertJobOperator.partial(
            task_id="clone_from_production",
            location=DEFAULT_LOCATION,
        ).expand(configuration=clone_config.output)

        log_clone_results = PythonOperator.partial(
            task_id="log_clone_results",
            python_callable=log_job_results_task,
        ).expand(op_kwargs=[{"job_id": clone_jobs.output}])

        prep_config >> [drop_config, clone_config]
        drop_config >> drop_jobs >> clone_jobs >> log_clone_results
        clone_config >> clone_jobs
