# app-project-migrate-tables-data – how data migration works

## Entry point: Component.php

```
Component::run()
  ├─ config.getDataMode() === 'database'  → DatabaseMigrate::migrate()
  └─ otherwise                            → SapiMigrate::migrate()
```

---

## SAPI mode

### SapiMigrate::migrate() flow

```
foreach tables (from configuration or getAllTables()):
  getTable($tableId)  →  tableInfo from source project

  skips: stage === 'sys'
  skips: isAlias === true

  if bucket does not exist in destination:
    storageModifier->createBucket()

  if table does not exist in destination:
    storageModifier->createTable(tableInfo)

  migrateTable(tableInfo, config)
```

### getAllTables() – table selection

If no whitelist (`migrateTables`) is defined, iterates through all source project buckets:
- Compares tables in source and destination projects
- Skips tables that already exist in destination and have `rowsCount > 0`
- Result is prepended to the array (newest bucket first)

### migrateTable() – standard flow

```
exportTableAsync(tableId, gzip: true, includeInternalTimestamp)
  → fileId

getFile(fileId)  →  fileInfo

if provider === 'gcp' AND isSliced AND sizeBytes > 50 GB:
  migrateGcsLargeTable->migrate()  ← see below

if isSliced:
  downloadSlicedFile(fileId, tmpFolder)  →  slices
  uploadSlicedFile(slices, federationToken)  →  destinationFileId

if non-sliced:
  downloadFile(fileId, fileName)
  uploadFile(fileName, federationToken)   →  destinationFileId

writeTableAsyncDirect(tableId, {dataFileId, columns, useTimestampFromDataFile})
```

### GCS large tables – parallel worker migration

Activation condition: source file is on GCS (`provider === 'gcp'`), is sliced and larger than **50 GB**.

```
MigrateGcsLargeTable::migrate(fileId, tableInfo, preserveTimestamp):

1. getFile(fileId, federationToken=true)
   → fileInfo with gcsPath (bucket + key) and gcsCredentials

2. GcsClient::bucket->object(manifest)->downloadAsString()
   → manifest with entries (list of chunk URLs)

3. array_chunk(entries, chunkSize=150)
   → chunks (groups of 150 slices)

4. Remove PK (if exists):
   targetClient->removeTablePrimaryKey(tableId)

5. Parallel loop (maxParallelism=3):

   PHASE 1 – Launching worker processes:
     while free slots:
       process = new Process([PHP, worker-chunk.php], stdin=json_encode(input))
       process.start()
       runningProcesses[chunkIndex] = process

     Collecting completed workers:
       if process.isSuccessful():
         result = json_decode(output)  →  {logs, fileId}
         writeQueue.push({fileId, chunkNum})
       else:
         errors.push(RuntimeException)

   PHASE 2 – Sequential SAPI upload (blocking):
     writeItem = writeQueue.shift()
     targetClient->writeTableAsyncDirect(tableId, {
       dataFileId: writeItem.fileId,
       columns: tableInfo.columns,
       incremental: true,               ← each chunk is appended
       useTimestampFromDataFile
     })

   usleep(100ms)  ← polling

6. After all chunks complete:
   Restore PK:
   targetClient->createTablePrimaryKey(tableId, primaryKey)
```

### worker-chunk.php

Child process for one GCS table chunk:

**Input (STDIN, JSON):**
```json
{
  "sourceApiUrl": "...", "sourceToken": "...",
  "targetApiUrl": "...", "targetToken": "...",
  "fileId": 12345,
  "bucket": "gcs-bucket-name",
  "chunk": [{"url": "gs://..."}, ...],
  "optionFileName": "in.c-bucket.table",
  "chunkNum": 1,
  "totalChunks": 5
}
```

**Output (STDOUT, JSON):**
```json
{
  "logs": ["Downloading slice 1/150 ...", ...],
  "fileId": "67890"
}
```

Worker downloads slices from GCS, uploads them to the destination project's SAPI file storage as a sliced file and returns `fileId`. The main process then uses this file in `writeTableAsyncDirect`.

### Why PK is removed and restored

Snowflake enforces PK uniqueness during `INSERT`. Incremental chunk uploads would cause duplicates (same row in multiple chunks after splitting slices). PK is therefore removed before import and restored after all chunks complete.

---

## Database mode (Snowflake native replication)

### Overview

Database mode uses native Snowflake database replication. Data is not transferred via SAPI – it goes directly Snowflake-to-Snowflake. Significantly faster for large data.

### DatabaseMigrate::migrate() flow

```
1. targetConnection.useRole('ACCOUNTADMIN')

2. (if shouldCreateReplicaDatabase)
   CREATE DATABASE IF NOT EXISTS <replicaDb>
     AS REPLICA OF <region>.<account>.<sourceDb>;

   Also tries lowercase database name variant (compatibility).

3. (if shouldRefreshReplicaDatabase)
   ALTER DATABASE <replicaDb> REFRESH;

4. targetConnection.useRole(original role)

5. (if shouldMigrateData)
   migrateData(config)

6. (if shouldDropReplicaDatabase)
   DROP DATABASE <replicaDb>;
```

### migrateData() – detail

```
SHOW SCHEMAS IN DATABASE <replicaDb>

skips schemas:
  - INFORMATION_SCHEMA
  - PUBLIC
  - READER_* (reader schemas)
  - WORKSPACE_* (unless in includedWorkspaceSchemas)

forEach schema:
  if bucket does not exist in SAPI: createBucket()
  migrateSchema(tablesWhiteList, schemaName)
  refreshTableInformationInBucket(schemaName)  ← notifies SAPI of update
```

### migrateTable() – detail

```
getSourceRole(connection, 'TABLE', schemaName.tableName)
  → SHOW GRANTS ON TABLE → owner (OWNERSHIP privilege)

grantRoleToMigrateUser(tableRole)
useRole(tableRole)
grantPrivilegesToReplicaDatabase(replicaDb, tableRole)

compareTableMaxTimestamp():
  compares MAX(_timestamp) in destination table vs. replicated table
  → if equal: table is up to date, skip

TRUNCATE TABLE <targetDb>.<schema>.<table>;

INSERT INTO <targetDb>.<schema>.<table> (<cols>)
  SELECT <cols>
  FROM <replicaDb>.<schema>.<table>;
```

### Skipped schemas in database mode

| Schema | Reason for skipping |
|--------|-----------------|
| `INFORMATION_SCHEMA` | Snowflake system schema |
| `PUBLIC` | Default empty schema |
| `READER_*` | Reader schemas for data sharing (Data Sharing) |
| `WORKSPACE_*` and `<number>_WORKSPACE*` | Workspaces (regex `^(\d+_)?WORKSPACE`) – included only if explicitly in `includedWorkspaceSchemas` |

### Predefined stack mappings

For standard Keboola stacks, the source Snowflake account and region are predefined in `Config.php` (constant `STACK_URL_TO_SNOWFLAKE_DATABASE`). For other stacks or BYODB, set manually:

```
isSourceByodb: true
sourceByodb: database_name
```

For BYODB there is one predefined value in `Config.php` (`BYODB_DATABASES`): `sourceByodb: "coates"` – other values are treated as direct database names.

### Sync action: createReplications

Before the first database mode migration between two Snowflake stacks, the source project must enable cross-account replication. Sync action `createReplications` (camelCase) runs:

```
DatabaseReplication::run():
  ALTER DATABASE <sourceDb>
    ENABLE REPLICATION TO ACCOUNTS <targetAccount>;
```

This action must be run in the **source** project using its SAPI token.

---

## Dry-run mode

If `dryRun: true`:
- No SAPI operations are performed (createBucket, createTable, writeTableAsyncDirect)
- No Snowflake SQL commands are executed (TRUNCATE, INSERT, CREATE DATABASE)
- Each skipped operation is logged as `[dry-run] ...`
