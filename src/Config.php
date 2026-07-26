<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfig;
use Keboola\Component\UserException;

class Config extends BaseConfig
{
    private const STACK_DATABASES = [
        'connection.keboola.com' => [
            'db_replica_prefix' => 'AWSUS',
            'db_prefix' => 'SAPI',
            'account' => 'KEBOOLA',
            'region' => 'AWS_US_WEST_2',
            'accountIdentifier' => 'RL74503.KEBOOLA_AWS_US_WEST_2',
        ],
        'connection.eu-central-1.keboola.com' => [
            'db_replica_prefix' => 'AWSEU',
            'db_prefix' => 'KEBOOLA',
            'account' => 'KEBOOLA',
            'region' => 'AWS_EU_CENTRAL_1',
            'accountIdentifier' => 'RL74503.KEBOOLA_AWS_EU_CENTRAL_1',
        ],
        'connection.north-europe.azure.keboola.com' => [
            'db_replica_prefix' => 'AZNE',
            'db_prefix' => 'KEBOOLA',
            'account' => 'KEBOOLA',
            'region' => 'AZURE_WESTEUROPE',
            'accountIdentifier' => 'RL74503.KEBOOLA_AZURE_WESTEUROPE',
        ],
        'connection.europe-west3.gcp.keboola.com' => [
            'db_replica_prefix' => 'GCPEUW3',
            'db_prefix' => 'KBC_EUW3',
            'account' => 'PJ41720',
            'region' => 'GCP_EUROPE_WEST3',
            'accountIdentifier' => 'RL74503.COM_KEBOOLA_GCP_EUROPE_WEST3_2',
        ],
        'connection.us-east4.gcp.keboola.com' => [
            'db_replica_prefix' => 'GCPUSE4',
            'db_prefix' => 'KBC_USE4',
            'account' => 'NE35810',
            'region' => 'GCP_US_EAST4',
            'accountIdentifier' => 'RL74503.COM_KEBOOLA_GCP_US_EAST4',
        ],
        'connection.coates.keboola.cloud' => [
            'db_replica_prefix' => 'COATESAWSUS',
            'db_prefix' => 'KBC',
            'account' => 'KEBOOLA',
            'region' => 'AWS_US_EAST_1',
        ],
    ];

    private const BYODB_DATABASES = [
        'coates' => [
            'db_replica_prefix' => 'COATESAWSUS',
            'db_prefix' => 'SAPI',
            'account' => 'COATES',
            'region' => 'AWS_US_EAST_1',
        ],
    ];

    public function getMode(): string
    {
        return $this->getStringValue(['parameters', 'mode']);
    }

    public function getSourceKbcUrl(): string
    {
        return $this->getStringValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceKbcToken(): string
    {
        return $this->getStringValue(['parameters', '#sourceKbcToken']);
    }

    public function getMigrateTables(): array
    {
        return $this->getArrayValue(['parameters', 'tables']);
    }

    public function getTargetHost(): string
    {
        return $this->getDbConfigNode()['host'];
    }

    public function getTargetUser(): string
    {
        return $this->getDbConfigNode()['username'];
    }

    public function getTargetPassword(): ?string
    {
        return $this->getDbConfigNode()['#password'] ?? null;
    }

    public function getTargetPrivateKey(): ?string
    {
        return $this->getDbConfigNode()['#privateKey'] ?? null;
    }

    public function getTargetWarehouse(): string
    {
        return $this->getDbConfigNode()['warehouse'];
    }

    public function getTargetWarehouseSize(): string
    {
        return $this->getDbConfigNode()['warehouse_size'] ?? 'SMALL';
    }

    public function getSourceDatabaseAccount(): string
    {
        if ($this->isSourceByodb()) {
            $sourceByodb = $this->getStringValue(['parameters', 'sourceByodb']);
            assert(array_key_exists($sourceByodb, self::BYODB_DATABASES));

            return self::BYODB_DATABASES[$sourceByodb]['account'];
        }
        $url = parse_url($this->getSourceKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::STACK_DATABASES[$url['host']]['account'];
    }

    public function getSourceDatabaseRegion(): string
    {
        if ($this->isSourceByodb()) {
            $sourceByodb = $this->getStringValue(['parameters', 'sourceByodb']);
            assert(array_key_exists($sourceByodb, self::BYODB_DATABASES));

            return self::BYODB_DATABASES[$sourceByodb]['region'];
        }
        $url = parse_url($this->getSourceKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::STACK_DATABASES[$url['host']]['region'];
    }

    public function getSourceDatabasePrefix(): string
    {
        if ($this->isSourceByodb()) {
            $sourceByodb =  $this->getStringValue(['parameters', 'sourceByodb']);
            assert(array_key_exists($sourceByodb, self::BYODB_DATABASES));

            return self::BYODB_DATABASES[$sourceByodb]['db_prefix'];
        }
        $url = parse_url($this->getSourceKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::STACK_DATABASES[$url['host']]['db_prefix'];
    }

    public function getReplicaDatabasePrefix(): string
    {
        if ($this->isSourceByodb()) {
            $sourceByodb = $this->getStringValue(['parameters', 'sourceByodb']);
            assert(array_key_exists($sourceByodb, self::BYODB_DATABASES));

            return self::BYODB_DATABASES[$sourceByodb]['db_replica_prefix'];
        }
        $url = parse_url($this->getSourceKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::STACK_DATABASES[$url['host']]['db_replica_prefix'];
    }

    public function getTargetDatabasePrefix(): string
    {
        $url = parse_url($this->getEnvKbcUrl());
        assert($url && array_key_exists('host', $url));

        return self::STACK_DATABASES[$url['host']]['db_prefix'];
    }

    public function getSourceHost(): string
    {
        return $this->getStringValue(['parameters', 'sourceHost']);
    }

    public function getSourceUser(): string
    {
        return $this->getStringValue(['parameters', 'sourceUsername']);
    }

    public function getSourcePassword(): string
    {
        return $this->getStringValue(['parameters', '#sourcePassword']);
    }

    public function getSourcePrivateKey(): string
    {
        return $this->getStringValue(['parameters', '#sourcePrivateKey']);
    }

    public function getProjectIdFrom(): int
    {
        return $this->getIntValue(['parameters', 'projectIdFrom']);
    }

    public function getProjectIdTo(): int
    {
        return $this->getIntValue(['parameters', 'projectIdTo']);
    }

    private function getDbConfigNode(): array
    {
        $paramDb = $this->getArrayValue(['parameters', 'db'], []);
        if ($paramDb) {
            return $paramDb;
        }
        return $this->getImageParameters()['db'];
    }

    public function isDryRun(): bool
    {
        return (bool) $this->getValue(['parameters', 'dryRun']);
    }

    public function isSourceByodb(): bool
    {
        return (bool) $this->getValue(['parameters',  'isSourceByodb']);
    }

    public function getIncludedWorkspaceSchemas(): array
    {
        return $this->getArrayValue(['parameters', 'includeWorkspaceSchemas']);
    }

    public function preserveTimestamp(): bool
    {
        return (bool) $this->getValue(['parameters', 'preserveTimestamp']);
    }

    public function isIncremental(): bool
    {
        return (bool) $this->getValue(['parameters', 'incremental']);
    }

    public function getChangedSince(): ?string
    {
        /** @var string|null $value */
        $value = $this->getValue(['parameters', 'changedSince']);
        return $value !== null ? $value : null;
    }

    public function shouldCreateReplicaDatabase(): bool
    {
        return (bool) $this->getValue(['parameters', 'replica', 'create'], true);
    }

    public function shouldRefreshReplicaDatabase(): bool
    {
        return (bool) $this->getValue(['parameters', 'replica', 'refresh'], true);
    }

    public function shouldDropReplicaDatabase(): bool
    {
        // A replication group is shared by all per-project runs migrating its member databases, so it
        // must not be torn down by an individual run — the orchestrator drops it once at the end (or a
        // run sets "replica.drop": true explicitly). Standalone replicas are per-run and dropped by default.
        return (bool) $this->getValue(['parameters', 'replica', 'drop'], !$this->useReplicationGroup());
    }

    public function shouldMigrateData(): bool
    {
        return (bool) $this->getValue(['parameters', 'migrateData'], true);
    }

    public function getReplicationStrategy(): string
    {
        return $this->getStringValue(['parameters', 'replicationStrategy'], 'standalone');
    }

    public function useReplicationGroup(): bool
    {
        return $this->getReplicationStrategy() === 'group';
    }

    /**
     * A replication group materializes its member databases under their original source names and
     * cannot rename them. If the source and target stacks share a database prefix, the replicated
     * member database could collide with a destination-owned database of the same name. Standalone
     * replication avoids this by renaming the local replica; the group strategy cannot, so it is
     * rejected for such stack pairs.
     */
    public function assertReplicationGroupStacksCompatible(): void
    {
        if (!$this->useReplicationGroup()) {
            return;
        }
        if ($this->getSourceDatabasePrefix() === $this->getTargetDatabasePrefix()) {
            throw new UserException(sprintf(
                'Replication group migration is not supported between stacks that share the database '
                . 'prefix "%s": the replicated member databases would collide with the destination\'s '
                . 'own databases (a replication group cannot rename its members). Use the "standalone" '
                . 'replication strategy for this stack pair.',
                $this->getSourceDatabasePrefix(),
            ));
        }
    }

    public function getReplicationGroupName(): string
    {
        return $this->getStringValue(['parameters', 'replicationGroup', 'name']);
    }

    public function getReplicationGroupSourceAccountIdentifier(): string
    {
        if ($this->isSourceByodb()) {
            $sourceByodb = $this->getStringValue(['parameters', 'sourceByodb']);
            $stack = self::BYODB_DATABASES[$sourceByodb] ?? [];
        } else {
            $url = parse_url($this->getSourceKbcUrl());
            if (!is_array($url) || !isset($url['host'])) {
                throw new UserException(sprintf(
                    'Could not parse host from source KBC URL "%s".',
                    $this->getSourceKbcUrl(),
                ));
            }
            $stack = self::STACK_DATABASES[$url['host']] ?? [];
        }

        if (empty($stack['accountIdentifier'])) {
            throw new UserException(sprintf(
                'Replication group mode is not supported for source stack "%s" '
                . '(missing Snowflake account identifier).',
                $this->getSourceKbcUrl(),
            ));
        }

        return $stack['accountIdentifier'];
    }

    /** @return string[] */
    public function getReplicationGroupDatabases(): array
    {
        return $this->getArrayValue(['parameters', 'replicationGroup', 'databases']);
    }

    public function forcePrimaryKeyNotNull(): bool
    {
        return (bool) $this->getValue(['parameters', 'forcePrimaryKeyNotNull']);
    }

    public function getGcsLargeTableParallelChunks(): int
    {
        return $this->getIntValue(['parameters', 'gcsLargeTable', 'parallelChunks'], 3);
    }

    public function getGcsLargeTableChunkSize(): int
    {
        return $this->getIntValue(['parameters', 'gcsLargeTable', 'chunkSize'], 150);
    }
}
