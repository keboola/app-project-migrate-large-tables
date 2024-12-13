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
        $this->createReplicaDatabase($config);
        $this->refreshReplicaDatabase($config);
        $this->targetConnection->useRole($currentRole);

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
                QueryBuilder::quoteIdentifier($config->getTargetWarehouse() . '_SMALL'),
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
            if (str_starts_with($schemaName, 'WORKSPACE')
                && !in_array($schemaName, $config->getIncludedWorkspaceSchemas())) {

                continue;
            }

            if (!$this->sourceSapiClient->bucketExists($schemaName)
                && !in_array($schemaName, $config->getIncludedExternalSchemas())) {

                continue;
            }

            if (!$this->targetSapiClient->bucketExists($schemaName)) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating bucket "%s".', $schemaName));
                } else {
                    // Create bucket
                    $this->logger->info(sprintf('Creating bucket "%s".', $schemaName));
                    $this->storageModifier->createBucket($schemaName);
                }
            }

            $this->migrateSchema($config->getMigrateTables(), $schemaName);
        }
        $this->dropReplicaDatabase();
    }

    private function migrateSchema(array $tablesWhiteList, string $schemaName): void
    {
        $this->logger->info(sprintf('Migrating schema %s', $schemaName));
        $currentRole = $this->targetConnection->getCurrentRole();
        $this->targetConnection->useRole('ACCOUNTADMIN');
        $tables = $this->targetConnection->fetchAll(sprintf(
            'SHOW TABLES IN SCHEMA %s.%s;',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
            QueryBuilder::quoteIdentifier($schemaName),
        ));
        $this->targetConnection->useRole($currentRole);

        foreach ($tables as $table) {
            $tableId = sprintf('%s.%s', $schemaName, $table['name']);
            if ($tablesWhiteList && !in_array($tableId, $tablesWhiteList, true)) {
                continue;
            }

            if ($this->dryRun) {
                $this->logger->info(sprintf('[dry-run] Migrating table %s.%s', $schemaName, $table['name']));
                continue;
            }

            if (!$this->targetSapiClient->tableExists($tableId)) {
                $this->logger->info(sprintf('Creating table "%s".', $tableId));
                $this->storageModifier->createTable(
                    $this->sourceSapiClient->getTable($tableId),
                );
            }

            $this->migrateTable($schemaName, $table['name']);
        }

        if ($this->dryRun === false) {
            $this->logger->info(sprintf('Refreshing table information in bucket %s', $schemaName));
            $this->targetSapiClient->refreshTableInformationInBucket($schemaName);
        } else {
            $this->logger->info(sprintf('[dry-run] Refreshing table information in bucket %s', $schemaName));
        }
    }

    private function migrateTable(string $schemaName, string $tableName): void
    {
        $this->logger->info(sprintf('Migrating table %s.%s', $schemaName, $tableName));
        $tableRole = $this->getSourceRole(
            $this->targetConnection,
            'TABLE',
            QueryBuilder::quoteIdentifier($schemaName) . '.' . QueryBuilder::quoteIdentifier($tableName),
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

        $columns = $this->targetConnection->getTableColumns($schemaName, $tableName);

        $compareTimestamp = $this->compareTableMaxTimestamp(
            'ACCOUNTADMIN',
            $tableRole,
            $this->replicaDatabase,
            $this->targetDatabase,
            $schemaName,
            $tableName,
        );

        if ($compareTimestamp) {
            $this->logger->info(sprintf('Table %s.%s is up to date', $schemaName, $tableName));
            return;
        }

        try {
            $this->targetConnection->query(sprintf(
                'TRUNCATE TABLE %s.%s.%s;',
                QueryBuilder::quoteIdentifier($this->targetDatabase),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));

            $this->targetConnection->query(sprintf(
                'INSERT INTO %s.%s.%s (%s) SELECT %s FROM %s.%s.%s;',
                QueryBuilder::quoteIdentifier($this->targetDatabase),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                implode(', ', array_map(fn($v) => QueryBuilder::quoteIdentifier($v), $columns)),
                QueryBuilder::quoteIdentifier($this->replicaDatabase),
                QueryBuilder::quoteIdentifier($schemaName),
                QueryBuilder::quoteIdentifier($tableName),
            ));
        } catch (RuntimeException $e) {
            $this->logger->warning(sprintf(
                'Error while migrating table %s.%s: %s',
                $schemaName,
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
        $this->targetConnection->query(sprintf(
            'CREATE DATABASE IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
            QueryBuilder::quoteIdentifier($this->replicaDatabase),
            $config->getSourceDatabaseRegion(),
            $config->getSourceDatabaseAccount(),
            QueryBuilder::quoteIdentifier($this->sourceDatabase),
        ));

        $this->logger->info(sprintf('Replica database %s created', $this->replicaDatabase));
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
        string $schema,
        string $table,
    ): bool {
        $sqlTemplate = 'SELECT max("_timestamp") as "maxTimestamp" FROM %s.%s.%s';

        $currentRole = $this->targetConnection->getCurrentRole();
        try {
            $this->targetConnection->useRole($firstDatabaseRole);

            $lastUpdateInFirstDatabase = $this->targetConnection->fetchAll(sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($firstDatabase),
                QueryBuilder::quoteIdentifier($schema),
                QueryBuilder::quoteIdentifier($table),
            ));

            $this->targetConnection->useRole($secondDatabaseRole);
            $lastUpdateInSecondDatabase = $this->targetConnection->fetchAll(sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($secondDatabase),
                QueryBuilder::quoteIdentifier($schema),
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
