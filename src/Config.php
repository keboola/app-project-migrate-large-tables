<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    private const STACK_DATABASES = [
        'connection.keboola.com' => [
            'db_replica_prefix' => 'AWSUS',
            'db_prefix' => 'SAPI',
            'account' => 'KEBOOLA',
            'region' => 'AWS_US_WEST_2',
        ],
        'connection.eu-central-1.keboola.com' => [
            'db_replica_prefix' => 'AWSEU',
            'db_prefix' => 'KEBOOLA',
            'account' => 'KEBOOLA',
            'region' => 'AWS_EU_CENTRAL_1',
        ],
        'connection.north-europe.azure.keboola.com' => [
            'db_replica_prefix' => 'AZNE',
            'db_prefix' => 'KEBOOLA',
            'account' => 'KEBOOLA',
            'region' => 'AZURE_WESTEUROPE',
        ],
        'connection.europe-west3.gcp.keboola.com' => [
            'db_replica_prefix' => 'GCPEUW3',
            'db_prefix' => 'KBC_EUW3',
            'account' => 'IK34405',
            'region' => 'GCP_EUROPE_WEST4',
        ],
        'connection.us-east4.gcp.keboola.com' => [
            'db_replica_prefix' => 'GCPUSE4',
            'db_prefix' => 'KBC_USE4',
            'account' => 'NE35810',
            'region' => 'GCP_US_EAST4',
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
        return (bool) $this->getValue(['parameters', 'replica', 'drop'], true);
    }

    public function shouldMigrateData(): bool
    {
        return (bool) $this->getValue(['parameters', 'migrateData'], true);
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
