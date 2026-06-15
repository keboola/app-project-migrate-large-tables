<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\SnowflakeDbAdapter\QueryBuilder;

class ReplicationGroup
{
    /**
     * @param string[] $databases Only needed on the primary side (ALLOWED_DATABASES);
     *                            the secondary side replicates the whole group by name.
     */
    public function __construct(
        private readonly string $name,
        private readonly array $databases = [],
    ) {
    }

    /** @return string[] */
    public function getDatabases(): array
    {
        return $this->databases;
    }

    public function createPrimarySql(string $targetAccountIdentifier): string
    {
        return sprintf(
            'CREATE REPLICATION GROUP %s OBJECT_TYPES = DATABASES '
            . 'ALLOWED_DATABASES = %s ALLOWED_ACCOUNTS = %s;',
            QueryBuilder::quoteIdentifier($this->name),
            $this->quotedDatabaseList(),
            $targetAccountIdentifier,
        );
    }

    public function createSecondarySql(string $sourceAccountIdentifier): string
    {
        return sprintf(
            'CREATE REPLICATION GROUP %s AS REPLICA OF %s.%s;',
            QueryBuilder::quoteIdentifier($this->name),
            $sourceAccountIdentifier,
            QueryBuilder::quoteIdentifier($this->name),
        );
    }

    public function refreshSql(): string
    {
        return sprintf(
            'ALTER REPLICATION GROUP %s REFRESH;',
            QueryBuilder::quoteIdentifier($this->name),
        );
    }

    public function dropSql(): string
    {
        return sprintf(
            'DROP REPLICATION GROUP IF EXISTS %s;',
            QueryBuilder::quoteIdentifier($this->name),
        );
    }

    public function alterAllowedDatabasesSql(): string
    {
        return sprintf(
            'ALTER REPLICATION GROUP %s SET ALLOWED_DATABASES = %s;',
            QueryBuilder::quoteIdentifier($this->name),
            $this->quotedDatabaseList(),
        );
    }

    public function alterAllowedAccountsSql(string $targetAccountIdentifier): string
    {
        return sprintf(
            'ALTER REPLICATION GROUP %s SET ALLOWED_ACCOUNTS = %s;',
            QueryBuilder::quoteIdentifier($this->name),
            $targetAccountIdentifier,
        );
    }

    private function quotedDatabaseList(): string
    {
        return implode(
            ', ',
            array_map(fn(string $db) => QueryBuilder::quoteIdentifier($db), $this->databases),
        );
    }
}
