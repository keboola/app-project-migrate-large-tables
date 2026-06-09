<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\Snowflake\Connection;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Psr\Log\LoggerInterface;

class DatabaseReplication
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly Connection $sourceConnection,
        private readonly Connection $targetConnection,
    ) {
    }

    public function createReplications(
        Config $config,
        int $fromProjectId,
        int $toProjectId,
    ): void {
        if ($config->useReplicationGroup()) {
            $this->createReplicationGroup(
                $config->getReplicationGroupName(),
                $config->getReplicationGroupDatabases(),
            );
            return;
        }

        $this->createStandaloneReplications($config, $fromProjectId, $toProjectId);
    }

    private function createStandaloneReplications(
        Config $config,
        int $fromProjectId,
        int $toProjectId,
    ): void {
        $databases = array_map(fn($v) => $v['name'], $this->sourceConnection->fetchAll('SHOW DATABASES;'));
        for ($i = $fromProjectId; $i <= $toProjectId; $i++) {
            $sourceDatabase = sprintf('%s_%s', $config->getSourceDatabasePrefix(), $i);
            if (!in_array($sourceDatabase, $databases, true)) {
                continue;
            }
            $this->createReplication($sourceDatabase);
        }
    }

    /**
     * @param string[] $databases
     */
    private function createReplicationGroup(string $name, array $databases): void
    {
        $group = new ReplicationGroup($name, $databases);

        $targetAccountIdentifier = sprintf(
            '%s.%s',
            $this->targetConnection->getOrgName(),
            $this->targetConnection->getAccountName(),
        );

        $this->logger->info(sprintf(
            'Creating replication group "%s" for databases: %s -> account %s',
            $name,
            implode(', ', $databases),
            $targetAccountIdentifier,
        ));

        try {
            $this->sourceConnection->query($group->createPrimarySql($targetAccountIdentifier));
        } catch (RuntimeException $e) {
            if (!str_contains($e->getMessage(), 'already exists')) {
                throw $e;
            }
            $this->logger->info(sprintf(
                'Replication group "%s" already exists, altering members instead.',
                $name,
            ));
            $this->alterReplicationGroupMembers($group, $targetAccountIdentifier);
        }
    }

    private function alterReplicationGroupMembers(
        ReplicationGroup $group,
        string $targetAccountIdentifier,
    ): void {
        $this->sourceConnection->query($group->alterAllowedDatabasesSql());
        $this->sourceConnection->query($group->alterAllowedAccountsSql($targetAccountIdentifier));
    }

    public function createReplication(string $sourceDatabase): void
    {
        // Allow replication on source database
        $this->logger->info(sprintf('Enabling replication on database %s', $sourceDatabase));

        $this->sourceConnection->query(sprintf(
            'ALTER DATABASE %s ENABLE REPLICATION TO ACCOUNTS %s.%s;',
            QueryBuilder::quoteIdentifier($sourceDatabase),
            QueryBuilder::quoteIdentifier($this->targetConnection->getRegion()),
            QueryBuilder::quoteIdentifier($this->targetConnection->getAccount()),
        ));
    }
}
