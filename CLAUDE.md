# app-project-migrate-tables-data – AI Development Context

## What this repository does

Keboola App component for direct table data migration between projects. Run by `app-project-migrate` as Phase 5 of the pipeline. Component ID: `keboola.app-project-migrate-large-tables`.

Has two modes: `sapi` (via Storage API) and `database` (via Snowflake replication). Database mode supports two replication strategies via `replicationStrategy`: `standalone` (default) and `group` (uses a Snowflake Replication Group to avoid materializing cross-DB zero-copy clones).

## Documentation

- **`docs/overview.md`** – sapi mode, database mode, GCS large tables, stack mappings
- **`docs/how-it-works.md`** – step-by-step migration flow (SAPI, parallel GCS workers, database replication)
- **`docs/configuration.md`** – complete input parameter reference for both modes

## Required environment variables

Before running tests, verify that these variables are present in `.env` (or exported in the shell). If missing, ask for them explicitly.

**Required to run tests:**

| Variable | Description |
|---|---|
| `SOURCE_CLIENT_URL` | URL of the source Keboola project |
| `SOURCE_CLIENT_TOKEN` | Storage token of the source project |
| `DESTINATION_CLIENT_URL` | URL of the destination Keboola project |
| `DESTINATION_CLIENT_TOKEN` | Storage token of the destination project |

**Only for local application run (not for tests):**

| Variable | Description |
|---|---|
| `KBC_URL` | Destination project URL (injected by Keboola platform) |
| `KBC_TOKEN` | Destination project token (injected by Keboola platform) |

> Tests use `SOURCE_CLIENT_*` and `DESTINATION_CLIENT_*`. `KBC_*` variables are only for running the application locally. Check that the `.env` file exists in the repo root.

## Development commands

Service name in `docker-compose.yml` is `dev`.

```bash
docker compose run --rm dev composer phpcs
docker compose run --rm dev composer phpstan
docker compose run --rm dev composer tests           # tests-phpunit + tests-datadir
docker compose run --rm dev composer tests-phpunit   # unit tests
docker compose run --rm dev composer tests-datadir   # functional tests
docker compose run --rm dev composer build           # phplint + phpcs + phpstan + tests
```

## Key files

| File | Purpose |
|---|---|
| `src/Component.php` | Entry point, mode routing + `createReplications` sync action registration |
| `src/Config.php` | Getters + predefined stack-to-Snowflake-DB mappings |
| `src/Configuration/ConfigDefinition.php` | Parameter validation for `run` action |
| `src/Configuration/CreateReplicationsConfigDefinition.php` | Parameter validation for `createReplications` sync action |
| `src/MigrateInterface.php` | Interface for strategy pattern (SapiMigrate, DatabaseMigrate) |
| `src/Strategy/SapiMigrate.php` | SAPI transfer logic |
| `src/Strategy/SapiMigrate/MigrateGcsLargeTable.php` | Parallel worker migration of large GCS tables |
| `src/Strategy/DatabaseMigrate.php` | Snowflake replication logic |
| `src/Strategy/DatabaseReplication.php` | Sync action for enabling replication |
| `src/Strategy/ReplicationGroup.php` | Builds Replication Group SQL (CREATE/ALTER/REFRESH/DROP) for database mode |
| `src/Snowflake/Connection.php` | Snowflake DB connection wrapper (grants, role handling) |
| `src/StorageModifier.php` | Creates buckets and tables in destination project (incl. cross-backend type mapping) |
| `src/worker-chunk.php` | Child process: downloads GCS chunk, uploads to SAPI |

## GCS large tables (>50 GB, sliced, GCP stack)

Parallel worker approach – manifest is split into chunks, each worker (`worker-chunk.php`) processes one chunk. Parameters: `gcsLargeTable.parallelChunks` (default 3, max 20), `gcsLargeTable.chunkSize` (default 150).

PK is removed before chunked import and restored after completion.

## Predefined stack mappings (src/Config.php)

Used in database mode:

| Stack | Snowflake account | Region |
|---|---|---|
| connection.keboola.com | KEBOOLA | AWS_US_WEST_2 |
| connection.eu-central-1.keboola.com | KEBOOLA | AWS_EU_CENTRAL_1 |
| connection.north-europe.azure.keboola.com | KEBOOLA | AZURE_WESTEUROPE |
| connection.europe-west3.gcp.keboola.com | IK34405 | GCP_EUROPE_WEST4 |
| connection.us-east4.gcp.keboola.com | NE35810 | GCP_US_EAST4 |
| connection.coates.keboola.cloud | KEBOOLA | AWS_US_EAST_1 |

## Cross-backend type mapping

When migrating Snowflake → BigQuery, `StorageModifier` maps column types. BigQuery NUMERIC max scale = 9.

## Coding standards

- PHP 8.x with strict types
- PHPStan level max
- Keboola coding standard (PSR-12)

## Related repositories

- Orchestrator: `app-project-migrate`
