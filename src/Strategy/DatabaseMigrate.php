<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\Snowflake\Connection;
use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;
use Throwable;

class DatabaseMigrate implements MigrateInterface
{
    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];
    private StorageModifier $storageModifier;

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly Connection $targetConnection,
        private readonly Client $sourceSapiClient,
        private readonly Client $targetSapiClient,
        private readonly string $sourceDatabase,
        private readonly string $replicaDatabase,
        private readonly string $targetDatabase,
        private readonly bool $dryRun = false,
    ) {
        $this->storageModifier = new StorageModifier($this->targetSapiClient);
    }

    public function migrate(Config $config): void
    {
        $currentRole = $this->targetConnection->getCurrentRole();
        $this->targetConnection->useRole('ACCOUNTADMIN');
        if ($config->shouldCreateReplicaDatabase()) {
            $this->createReplicaDatabase($config);
        }
        if ($config->shouldRefreshReplicaDatabase()) {
            $this->refreshReplicaDatabase($config);
        }
        $this->targetConnection->useRole($currentRole);

        if ($config->shouldMigrateData()) {
            $this->migrateData($config);
        }

        if ($config->shouldDropReplicaDatabase()) {
            $this->dropReplicaDatabase();
        }
    }

    public function migrateData(Config $config): void
    {
        $databaseRole = $this->getSourceRole(
            $this->targetConnection,
            'DATABASE',
            QueryBuilder::quoteIdentifier($this->targetDatabase),
        );
        $this->targetConnection->grantRoleToMigrateUser($databaseRole);
        $this->targetConnection->useRole($databaseRole);

        $hasDynamicBackend = in_array(
            'workspace-snowflake-dynamic-backend-size',
            $this->targetSapiClient->verifyToken()['owner']['features'],
        );

        if ($hasDynamicBackend) {
            $this->targetConnection->query(sprintf(
                'USE WAREHOUSE %s',
                QueryBuilder::quoteIdentifier(sprintf(
                    '%s_%s',
                    $config->getTargetWarehouse(),
                    $config->getTargetWarehouseSize(),
                )),
            ));
        }

        $this->targetConnection->query(sprintf(
            'USE DATABASE %s;',
            QueryBuilder::quoteIdentifier($this->targetDatabase),
        ));

        $currentRole = $this->targetConnection->getCurrentRole();
        $this->targetConnection->useRole('ACCOUNTADMIN');
        $schemas = $this->targetConnection->fetchAll(sprintf(
            'SHOW SCHEMAS IN DATABASE %s;',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
        ));
        $this->targetConnection->useRole($currentRole);

        foreach ($schemas as $schema) {
            $schemaName = $schema['name'];
            if (in_array($schemaName, self::SKIP_CLONE_SCHEMAS, true)) {
                continue;
            }
            if (preg_match('/^READER_/', $schemaName)) {
                $this->logger->info(sprintf('Skipping reader schema "%s".', $schemaName));
                continue;
            }
            if (preg_match('/^(\d+_)?WORKSPACE/', $schemaName)
                && !in_array($schemaName, $config->getIncludedWorkspaceSchemas())) {
                continue;
            }

            // Workspace schemas (e.g. "WORKSPACE_1166207470") don't have a dot separator.
            // Regular schemas (e.g. "in.c-bucket-name") have the stage prefix and work as both
            // Snowflake schema names and SAPI bucket IDs.
            // For workspace schemas, we need to compute separate SAPI bucket IDs.
            $isWorkspaceSchema = !str_contains($schemaName, '.');

            if ($isWorkspaceSchema) {
                // Source project has external bucket "in.WORKSPACE_xxx"
                $sourceSapiBucketId = 'in.' . $schemaName;
            } else {
                $sourceSapiBucketId = $schemaName;
            }

            // Determine target SAPI bucket ID
            $targetSapiBucketId = null;
            if ($isWorkspaceSchema) {
                // Check if a bucket matching this workspace schema already exists
                // SAPI createBucket adds "c-" prefix, so target bucket will be "in.c-WORKSPACE_xxx"
                $targetSapiBucketId = 'in.c-' . $schemaName;
            } else {
                $targetSapiBucketId = $schemaName;
            }

            if (!$this->targetSapiClient->bucketExists($targetSapiBucketId)) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating bucket "%s".', $schemaName));
                } else {
                    // Create bucket
                    $this->logger->info(sprintf('Creating bucket "%s".', $schemaName));
                    $targetSapiBucketId = $this->storageModifier->createBucket($schemaName);
                }
            }

            // For Snowflake SQL on target, the schema name matches the SAPI bucket ID
            $targetSchemaName = $targetSapiBucketId ?? $schemaName;

            $this->migrateSchema(
                $config->getMigrateTables(),
                $schemaName,
                $sourceSapiBucketId,
                $targetSapiBucketId ?? $schemaName,
                $targetSchemaName,
            );
        }
    }

    private function migrateSchema(
        array $tablesWhiteList,
        string $replicaSchemaName,
        string $sourceSapiBucketId,
        string $targetSapiBucketId,
        string $targetSchemaName,
    ): void {
        $this->logger->info(sprintf('Migrating schema %s', $replicaSchemaName));
        $currentRole = $this->targetConnection->getCurrentRole();
        $this->targetConnection->useRole('ACCOUNTADMIN');
        $tables = $this->targetConnection->fetchAll(sprintf(
            'SHOW TABLES IN SCHEMA %s.%s;',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
            QueryBuilder::quoteIdentifier($replicaSchemaName),
        ));
        $this->targetConnection->useRole($currentRole);

        foreach ($tables as $table) {
            // Whitelist uses replica schema name format (e.g. "WORKSPACE_xxx.TABLE" or "in.c-bucket.TABLE")
            $whitelistTableId = sprintf('%s.%s', $replicaSchemaName, $table['name']);
            if ($tablesWhiteList && !in_array($whitelistTableId, $tablesWhiteList, true)) {
                continue;
            }

            if ($this->dryRun) {
                $this->logger->info(sprintf('[dry-run] Migrating table %s.%s', $replicaSchemaName, $table['name']));
                continue;
            }

            // SAPI table IDs use the bucket ID format
            $sourceTableId = sprintf('%s.%s', $sourceSapiBucketId, $table['name']);
            $targetTableId = sprintf('%s.%s', $targetSapiBucketId, $table['name']);

            if (!$this->targetSapiClient->tableExists($targetTableId)) {
                $this->logger->info(sprintf('Creating table "%s".', $targetTableId));
                $tableInfo = $this->sourceSapiClient->getTable($sourceTableId);
                // Override bucket ID and table ID to match the target bucket
                $tableInfo['bucket']['id'] = $targetSapiBucketId;
                $tableInfo['id'] = $targetTableId;
                $this->storageModifier->createTable($tableInfo);
            }

            $this->migrateTable($replicaSchemaName, $targetSchemaName, $table['name']);
        }

        if ($this->dryRun === false) {
            $this->logger->info(sprintf('Refreshing table information in bucket %s', $targetSapiBucketId));
            $this->targetSapiClient->refreshTableInformationInBucket($targetSapiBucketId);
        } else {
            $this->logger->info(sprintf('[dry-run] Refreshing table information in bucket %s', $targetSapiBucketId));
        }
    }

    private function migrateTable(
        string $replicaSchemaName,
        string $targetSchemaName,
        string $tableName,
    ): void {
        $this->logger->info(sprintf('Migrating table %s.%s', $replicaSchemaName, $tableName));
        // Get ownership role from the target database schema
        $tableRole = $this->getSourceRole(
            $this->targetConnection,
            'TABLE',
            QueryBuilder::quoteIdentifier($targetSchemaName) . '.' . QueryBuilder::quoteIdentifier($tableName),
        );

        try {
            $this->targetConnection->useRole($tableRole);
        } catch (Throwable) {
            $this->targetConnection->grantRoleToMigrateUser($tableRole);
            $this->targetConnection->useRole($tableRole);
        }

        $this->targetConnection->grantPrivilegesToReplicaDatabase(
            $this->replicaDatabase,
            $tableRole,
        );

        // Get columns from the target database schema
        $columns = $this->targetConnection->getTableColumns($targetSchemaName, $tableName);

        $compareTimestamp = $this->compareTableMaxTimestamp(
            'ACCOUNTADMIN',
            $tableRole,
            $this->replicaDatabase,
            $this->targetDatabase,
            $replicaSchemaName,
            $targetSchemaName,
            $tableName,
        );

        if ($compareTimestamp) {
            $this->logger->info(sprintf('Table %s.%s is up to date', $replicaSchemaName, $tableName));
            return;
        }

        try {
            $this->targetConnection->query(sprintf(
                'TRUNCATE TABLE %s.%s.%s;',
                QueryBuilder::quoteIdentifier($this->targetDatabase),
                QueryBuilder::quoteIdentifier($targetSchemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));

            $this->targetConnection->query(sprintf(
                'INSERT INTO %s.%s.%s (%s) SELECT %s FROM %s.%s.%s;',
                QueryBuilder::quoteIdentifier($this->targetDatabase),
                QueryBuilder::quoteIdentifier($targetSchemaName),
                QueryBuilder::quoteIdentifier($tableName),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                QueryBuilder::quoteIdentifier($this->replicaDatabase),
                QueryBuilder::quoteIdentifier($replicaSchemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));
        } catch (RuntimeException $e) {
            $this->logger->warning(sprintf(
                'Error while migrating table %s.%s: %s',
                $replicaSchemaName,
                $tableName,
                $e->getMessage(),
            ));
            return;
        }
    }

    private function getSourceRole(Connection $connection, string $showGrantsOn, string $targetSourceName): string
    {
        $grantsOnDatabase = $connection->fetchAll(sprintf(
            'SHOW GRANTS ON %s %s;',
            $showGrantsOn,
            $targetSourceName,
        ));

        $ownershipOnDatabase = array_filter($grantsOnDatabase, fn($v) => $v['privilege'] === 'OWNERSHIP');
        assert(count($ownershipOnDatabase) === 1);

        return current($ownershipOnDatabase)['grantee_name'];
    }

    private function createReplicaDatabase(Config $config): void
    {
        // Migration database sqls
        $this->logger->info(sprintf('Creating replica database %s', $this->replicaDatabase));

        $databaseVariants = [
            $this->sourceDatabase,
            strtolower($this->sourceDatabase),
        ];

        $lastException = null;
        foreach ($databaseVariants as $dbName) {
            try {
                $this->targetConnection->query(sprintf(
                    'CREATE DATABASE IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
                    QueryBuilder::quoteIdentifier($this->replicaDatabase),
                    $config->getSourceDatabaseRegion(),
                    $config->getSourceDatabaseAccount(),
                    QueryBuilder::quoteIdentifier($dbName),
                ));

                $this->logger->info(sprintf('Replica database %s created', $this->replicaDatabase));
                return;
            } catch (RuntimeException $e) {
                $lastException = $e;
                $this->logger->warning(sprintf(
                    'Failed to create replica with database name %s: %s',
                    $dbName,
                    $e->getMessage(),
                ));
            }
        }

        throw new RuntimeException(
            'Failed to create replica database with any database name variant',
            0,
            $lastException,
        );
    }

    private function refreshReplicaDatabase(Config $config): void
    {
        $this->targetConnection->query(sprintf(
            'USE DATABASE %s',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
        ));
        $this->targetConnection->query('USE SCHEMA PUBLIC');

        $this->targetConnection->query(sprintf(
            'USE WAREHOUSE %s',
            QueryBuilder::quoteIdentifier($config->getTargetWarehouse()),
        ));

        // Run replicate of data
        $this->logger->info(sprintf('Refreshing replica database %s', $this->replicaDatabase));
        $this->targetConnection->query(sprintf(
            'ALTER DATABASE %s REFRESH',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
        ));
    }

    private function dropReplicaDatabase(): void
    {
        $this->targetConnection->useRole('ACCOUNTADMIN');
        $this->targetConnection->query(sprintf(
            'DROP DATABASE %s;',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
        ));
    }

    private function compareTableMaxTimestamp(
        string $firstDatabaseRole,
        string $secondDatabaseRole,
        string $firstDatabase,
        string $secondDatabase,
        string $firstSchema,
        string $secondSchema,
        string $table,
    ): bool {
        $sqlTemplate = 'SELECT max("_timestamp") as "maxTimestamp" FROM %s.%s.%s';

        $currentRole = $this->targetConnection->getCurrentRole();
        try {
            $this->targetConnection->useRole($firstDatabaseRole);

            $lastUpdateInFirstDatabase = $this->targetConnection->fetchAll(sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($firstDatabase),
                QueryBuilder::quoteIdentifier($firstSchema),
                QueryBuilder::quoteIdentifier($table),
            ));

            $this->targetConnection->useRole($secondDatabaseRole);
            $lastUpdateInSecondDatabase = $this->targetConnection->fetchAll(sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($secondDatabase),
                QueryBuilder::quoteIdentifier($secondSchema),
                QueryBuilder::quoteIdentifier($table),
            ));
        } catch (RuntimeException $e) {
            return false;
        } finally {
            $this->targetConnection->useRole($currentRole);
        }

        return $lastUpdateInFirstDatabase[0]['maxTimestamp'] === $lastUpdateInSecondDatabase[0]['maxTimestamp'];
    }
}
