# Configuration parameter reference – app-project-migrate-tables-data

## Required parameters

| Parameter | Description |
|---|---|
| `sourceKbcUrl` | URL of the source Keboola project |
| `#sourceKbcToken` | Storage token of the source project |

## General parameters

| Parameter | Default | Description |
|---|---|---|
| `mode` | `sapi` | Migration mode: `sapi` (via Storage API) or `database` (via Snowflake replication) |
| `dryRun` | `false` | Simulation without actual changes |
| `preserveTimestamp` | `false` | Preserves `_timestamp` column values from source data |
| `forcePrimaryKeyNotNull` | `false` | Forces NOT NULL on PK columns in typed tables |
| `tables` | `[]` | Whitelist of table IDs to migrate – if empty, all tables are migrated |
| `migrateData` | `true` | In database mode: performs TRUNCATE + INSERT from replica to destination. Can be disabled for preparation phase (only CREATE REPLICA + REFRESH, no data transfer). |

## SAPI mode – specific parameters

| Parameter | Default | Description |
|---|---|---|
| `gcsLargeTable.parallelChunks` | `3` | Number of parallel worker processes for large GCS tables (min 1, max 20) |
| `gcsLargeTable.chunkSize` | `150` | Number of GCS slices per worker chunk (min 1) |

> Large GCS table = sliced file larger than 50 GB on GCP storage backend.

## Database mode – specific parameters

Required if `mode: database`. Ignored if `mode: sapi`.

### Snowflake credentials

| Parameter | Required | Description |
|---|---|---|
| `db.host` | ✓ | Snowflake host (e.g. `xy12345.snowflakecomputing.com`) |
| `db.username` | ✓ | Snowflake username |
| `db.#password` | one of | Password |
| `db.#privateKey` | one of | Private key for key-pair authentication |
| `db.warehouse` | ✓ | Snowflake warehouse name |
| `db.warehouse_size` | `SMALL` | Warehouse size: `SMALL`, `MEDIUM`, or `LARGE` |

### BYODB parameters

| Parameter | Default | Description |
|---|---|---|
| `isSourceByodb` | `false` | Source uses a non-standard Snowflake account (BYODB) |
| `sourceByodb` | – | BYODB database name (required if `isSourceByodb: true`) |
| `includeWorkspaceSchemas` | `[]` | Workspace schemas to include in database migration |

### Replication switches

| Parameter | Default | Description |
|---|---|---|
| `replica.create` | `true` | Creates replica database |
| `replica.refresh` | `true` | Refreshes replica before migration |
| `replica.drop` | `true` (standalone) / `false` (group) | Drops the replica after migration. In `group` mode the default is `false`: the replication group is shared by all per-project runs migrating its member databases, so a single run must not tear it down — drop it once after all databases are migrated (or set `true` explicitly). |

> The TRUNCATE + INSERT from replica to destination is controlled by the `migrateData` parameter (see General parameters) – it is at the `parameters` level, not under `replica`.

> Predefined stack-to-Snowflake-account mappings are in `src/Config.php`. To add a new stack, extend this mapping.

### Replication strategy

| Parameter | Default | Description |
|---|---|---|
| `replicationStrategy` | `standalone` | `standalone` = per-database replication (default, backward compatible). `group` = Snowflake Replication Group, preserves cross-database zero-copy clones to avoid storage inflation. |
| `replicationGroup.name` | – | Replication group name. Required when `replicationStrategy: group`. Must be the SAME name on the primary (`createReplications`) and secondary (`run`) sides — the caller supplies it. |
| `replicationGroup.databases` | – | **`createReplications` action only.** Explicit list of source databases that form the group (used as `ALLOWED_DATABASES`). Required there when `replicationStrategy: group`. The `run` action does **not** take this — each run migrates the single project database it resolves from `projectId` the same way standalone does. |

> The source account identifier (`org_name.account_name` format, used by the run action for `CREATE REPLICATION GROUP … AS REPLICA OF …`) is **derived automatically** from `sourceKbcUrl` via the predefined stack mapping in `src/Config.php` — the same mechanism standalone replication uses. It is not a configuration parameter. If `replicationStrategy: group` is used on a stack that has no account identifier in the mapping, the run action fails with a clear error.

> **Same-prefix restriction.** A replication group materializes member databases under their original source names and cannot rename them (unlike standalone, which renames the local replica). To prevent the replicated database from colliding with a destination-owned database, `replicationStrategy: group` is **rejected when the source and target stacks share the same `db_prefix`** in the stack mapping (e.g. `connection.eu-central-1.keboola.com` and `connection.north-europe.azure.keboola.com` both use `KEBOOLA`). Use `standalone` for such stack pairs.

## Configuration examples

### SAPI mode (default)

```json
{
  "parameters": {
    "mode": "sapi",
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx"
  }
}
```

### SAPI mode with increased parallelism for large GCS tables

```json
{
  "parameters": {
    "mode": "sapi",
    "sourceKbcUrl": "https://connection.europe-west3.gcp.keboola.com",
    "#sourceKbcToken": "xxx",
    "gcsLargeTable": {
      "parallelChunks": 10,
      "chunkSize": 200
    }
  }
}
```

### SAPI mode – selected tables only

```json
{
  "parameters": {
    "mode": "sapi",
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx",
    "tables": [
      "in.c-my-bucket.my-table",
      "in.c-my-bucket.another-table"
    ]
  }
}
```

### Database mode (Snowflake replication)

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx",
    "db": {
      "host": "keboola.snowflakecomputing.com",
      "username": "svc_migrate",
      "#password": "secret",
      "warehouse": "MIGRATE_WH",
      "warehouse_size": "MEDIUM"
    }
  }
}
```

### Database mode with BYODB source

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx",
    "isSourceByodb": true,
    "sourceByodb": "CUSTOMER_DB_NAME",
    "db": {
      "host": "keboola.snowflakecomputing.com",
      "username": "svc_migrate",
      "#password": "secret",
      "warehouse": "MIGRATE_WH"
    }
  }
}
```

### Database mode with Replication Group

```json
{
  "parameters": {
    "mode": "database",
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx",
    "replicationStrategy": "group",
    "replicationGroup": {
      "name": "MIGRATE_RG_1234"
    },
    "db": {
      "host": "keboola.snowflakecomputing.com",
      "username": "svc_migrate",
      "#password": "secret",
      "warehouse": "MIGRATE_WH"
    }
  }
}
```

### Dry run (simulation)

```json
{
  "parameters": {
    "mode": "sapi",
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx",
    "dryRun": true
  }
}
```
