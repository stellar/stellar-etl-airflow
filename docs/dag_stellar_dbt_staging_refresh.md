# dag_stellar_dbt_staging_refresh

DAG to clone production BigQuery tables/views into staging. Supports three run modes (`schema`, `datasets`, `tables_views`), optional time-travel clones for tables, and per-job logging of BigQuery results. All jobs run in location `US`.

## How it runs
- Tasks: `prepare_clone_config` → (`build_drop_jobs`, `build_clone_jobs`) → `drop_staging_objects` → `clone_from_production` → `log_clone_results`.
- Drop jobs: one per target dataset in `schema`/`datasets`; single drop job (staging defaults) in `tables_views`.
- Clone jobs: one per source dataset (`schema`/`datasets`) or one script for all manual tables/views.
- Logging: `log_clone_results` fetches each BigQuery job’s result rows and logs `kind` + `name` (`cloned_table`, `cloned_view`, `missing_table`, `missing_view`, `missing_dataset`).
- Location: always `US` for BigQuery jobs and result fetches.

## Config surface
- Common params
  - `mode`: `schema` | `datasets` | `tables_views` (default `schema`).
  - `drop_staging_objects`: boolean, default `true`.
  - `time_travel_hours`: optional non-negative int; applies to table clones (views ignore).
  - `target_project` / `target_dataset`: defaults from Airflow vars `bq_project` / `bq_dataset`.
- Guards & rules
  - Disallowed target projects: `crypto-stellar`, `hubble-261722` (must write to staging).
  - Schema mode skips names matching `_.*bkp_[0-9]{8}`.
  - Missing datasets/tables/views are logged, not fatal.
- Airflow variables used
  - `bq_project`, `bq_dataset` (staging defaults).
  - `staging_clone_sources` (schema mode default sources list).
  - `sentry_environment` gate (only builds DAG for `production`/`staging`).

## Modes
- `schema`
  - Sources from `staging_clone_sources` unless `sources` provided inline.
  - Builds one clone job per dataset; per-target drop once per dataset.
  - Optional `time_travel_hours` applies to table clones.
- `datasets`
  - Provide `datasets` array of `{project,dataset[,target_project,target_dataset]}` or single `project`/`dataset`.
  - Each dataset becomes its own clone job; per-target drop once.
  - Optional `time_travel_hours` applies to table clones.
- `tables_views`
  - Manual lists: `tables` array of `{project,dataset,table[,target_project,target_dataset]}`; `views` array of `{project,dataset,view[,target_project,target_dataset]}`.
  - Convenience single `table`/`view` keys are appended to the arrays.
  - One clone script handles all entries; one drop job using `target_project`/`target_dataset` defaults.
  - Optional `time_travel_hours` applies to tables only.

## Test payloads (drop flags set explicitly)
Use in `dag_run.conf` when triggering.

### Schema mode (no drop)
```json
{
  "mode": "schema",
  "drop_staging_objects": false
}
```

### Datasets mode (1 dataset: raw)
```json
{
  "mode": "datasets",
  "drop_staging_objects": true,
  "datasets": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_raw",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_raw"
    }
  ]
}
```

### Datasets mode (2 datasets)
```json
{
  "mode": "datasets",
  "drop_staging_objects": true,
  "datasets": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_raw",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_raw"
    },
    {
      "project": "hubble-261722",
      "dataset": "dbt_references",
      "target_project": "staging-hubble",
      "target_dataset": "staging_dbt_references"
    }
  ]
}
```

### tables_views (1 table, 1 view)
```json
{
  "mode": "tables_views",
  "drop_staging_objects": false,
  "tables": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_raw",
      "table": "account_signers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_raw"
    }
  ],
  "views": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar",
      "view": "recent_ledgers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar"
    }
  ]
}
```

### tables_views (2 tables, 1 view)
```json
{
  "mode": "tables_views",
  "drop_staging_objects": false,
  "tables": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_raw",
      "table": "account_signers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_raw"
    },
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar",
      "table": "config_settings",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar"
    }
  ],
  "views": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar",
      "view": "recent_ledgers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar"
    }
  ]
}
```

### tables_views (1 table, 2 views)
```json
{
  "mode": "tables_views",
  "drop_staging_objects": false,
  "tables": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_raw",
      "table": "account_signers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_raw"
    }
  ],
  "views": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar",
      "view": "recent_ledgers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar"
    },
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_dbt",
      "view": "some_small_view",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_dbt"
    }
  ]
}
```

### tables_views (tables only, time travel 2h)
```json
{
  "mode": "tables_views",
  "drop_staging_objects": true,
  "time_travel_hours": 2,
  "tables": [
    {
      "project": "crypto-stellar",
      "dataset": "crypto_stellar_raw",
      "table": "account_signers",
      "target_project": "staging-hubble",
      "target_dataset": "staging_crypto_stellar_raw"
    }
  ],
  "views": []
}
```

## Notes
- `tables_views` drop uses the staging defaults (`target_project`/`target_dataset`) once per run; `schema`/`datasets` drop per target dataset.
- All jobs run in `US` location.
- Time travel applies only to table clones.
- Missing sources are reported in log results; job succeeds even when some objects are missing.
- Avoid targeting production projects; the DAG blocks known prod projects via guardrails.
