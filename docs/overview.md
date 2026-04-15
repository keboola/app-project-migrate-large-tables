# app-project-migrate-tables-data – overview

## What the component does

Migrates table row data from a source Keboola project to a destination. Run by `app-project-migrate` as Phase 5 of the migration pipeline. Component ID: `keboola.app-project-migrate-large-tables`.

Has two modes: `sapi` (default, via Storage API) and `database` (direct Snowflake replication).

## Migration modes

### sapi

Data flows through Storage API: export from source → upload to shared SAPI file storage → import to destination.

For large GCS tables (sliced + >50 GB), parallel worker processes (`worker-chunk.php`) are used instead of the standard flow.

### database

Uses native Snowflake database replication (`CREATE DATABASE AS REPLICA OF ...`). Significantly faster than sapi mode for large data when migrating between Snowflake stacks.

## What is skipped (sapi mode)

- **Sys bucket** tables (`stage === 'sys'`)
- **Alias** tables

> External schema and Data Catalog tables are not explicitly skipped in this component – the check happens earlier during the backup phase in `php-kbc-project-backup`. If you migrate a table whitelist, it is your responsibility to exclude them.

## Predefined stack mappings (database mode)

| Stack URL | Snowflake account | Region |
|---|---|---|
| `connection.keboola.com` | KEBOOLA | AWS_US_WEST_2 |
| `connection.eu-central-1.keboola.com` | KEBOOLA | AWS_EU_CENTRAL_1 |
| `connection.north-europe.azure.keboola.com` | KEBOOLA | AZURE_WESTEUROPE |
| `connection.europe-west3.gcp.keboola.com` | IK34405 | GCP_EUROPE_WEST4 |
| `connection.us-east4.gcp.keboola.com` | NE35810 | GCP_US_EAST4 |
| `connection.coates.keboola.cloud` | KEBOOLA | AWS_US_EAST_1 |

For other stacks or BYODB: `isSourceByodb: true` + `sourceByodb: <database_name>`.

## Sync action: createReplications

`DatabaseReplication` enables Snowflake cross-account replication (`ALTER DATABASE ... ENABLE REPLICATION TO ACCOUNTS ...`). Must be run before the first database mode migration between two stacks. The action name is `createReplications` (camelCase), as registered in `Component::getSyncActions()`.

## Architecture

```
Component.php
  ├─ SapiMigrate
  │    ├─ StorageModifier     – creates buckets and tables in destination
  │    └─ MigrateGcsLargeTable
  │         └─ worker-chunk.php  (child process, one per chunk)
  ├─ DatabaseMigrate
  │    └─ Snowflake/Connection
  └─ DatabaseReplication      (sync action)
```

## Key files

| File | Description |
|---|---|
| `src/Component.php` | Entry point, mode routing |
| `src/Config.php` | Configuration getters + predefined stack-to-Snowflake-DB mappings |
| `src/ConfigDefinition.php` | Parameter validation |
| `src/Strategy/SapiMigrate.php` | SAPI transfer logic |
| `src/Strategy/SapiMigrate/MigrateGcsLargeTable.php` | Parallel worker migration of large GCS tables |
| `src/Strategy/DatabaseMigrate.php` | Snowflake replication logic |
| `src/Strategy/DatabaseReplication.php` | Sync action for enabling replication |
| `src/StorageModifier.php` | Creates buckets and tables in destination project |
| `src/worker-chunk.php` | Child process: downloads GCS chunk, uploads to SAPI |

## Development and testing

```bash
docker compose run --rm dev composer phpcs
docker compose run --rm dev composer phpstan
docker compose run --rm dev composer tests
```

## Related repositories

- Used by: `app-project-migrate` (Phase 5)
- Component ID: `keboola.app-project-migrate-large-tables`
