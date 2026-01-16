<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception as StorageApiException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\Temp\Temp;

class StorageModifier
{
    private Temp $tmp;

    public function __construct(readonly Client $client)
    {
        $this->tmp = new Temp();
    }

    public function createBucket(string $schemaName): void
    {
        [$bucketStage, $bucketName] = explode('.', $schemaName);
        if (str_starts_with($bucketName, 'c-')) {
            $bucketName = substr($bucketName, 2);
        }
        $this->client->createBucket($bucketName, $bucketStage);
    }

    public function createTable(array $tableInfo): void
    {
        if ($tableInfo['isTyped']) {
            $this->createTypedTable($tableInfo);
        } else {
            $this->createNonTypedTable($tableInfo);
        }

        $this->restoreTableColumnsMetadata($tableInfo, $tableInfo['id'], new Metadata($this->client));
    }

    private function createNonTypedTable(array $tableInfo): void
    {
        $tempFile = $this->tmp->createFile(sprintf('%s.header.csv', $tableInfo['id']));
        $headerFile = new CsvFile($tempFile->getPathname());
        $headerFile->writeRow($tableInfo['columns']);

        $this->client->createTableAsync(
            $tableInfo['bucket']['id'],
            $tableInfo['name'],
            $headerFile,
            [
                'primaryKey' => join(',', $tableInfo['primaryKey']),
            ],
        );
    }

    private function createTypedTable(array $tableInfo): void
    {
        $sourceBackend = $tableInfo['bucket']['backend'];
        $destinationBackend = $this->getDestinationBucketBackend($tableInfo['bucket']['id']);

        $columns = [];
        foreach ($tableInfo['columns'] as $column) {
            $columns[$column] = [];
        }
        foreach ($tableInfo['columnMetadata'] ?? [] as $columnName => $column) {
            $columnName = (string) $columnName;
            $columnMetadata = [];
            foreach ($column as $metadata) {
                if ($metadata['provider'] !== 'storage') {
                    continue;
                }
                $columnMetadata[$metadata['key']] = $metadata['value'];
            }

            if ($destinationBackend !== $sourceBackend) {
                // Cross-backend migration: use basetype to get correct native type for target backend
                $sourceBaseType = $this->getBaseType(
                    $sourceBackend,
                    $columnMetadata['KBC.datatype.type'],
                );
                $definition = $this->getDefinitionForBasetype($destinationBackend, $sourceBaseType);
                // Preserve nullable property from source
                $definition['nullable'] = $columnMetadata['KBC.datatype.nullable'] === '1';
            } else {
                // Same backend: use original type directly
                $definition = [
                    'type' => $columnMetadata['KBC.datatype.type'],
                    'nullable' => $columnMetadata['KBC.datatype.nullable'] === '1',
                ];
                if (isset($columnMetadata['KBC.datatype.length'])) {
                    $definition['length'] = $columnMetadata['KBC.datatype.length'];
                }
                if (isset($columnMetadata['KBC.datatype.default'])) {
                    $definition['default'] = $columnMetadata['KBC.datatype.default'];
                }
            }

            $columns[$columnName] = [
                'name' => $columnName,
                'definition' => $definition,
                'basetype' => $columnMetadata['KBC.datatype.basetype'],
            ];
        }

        $data = [
            'name' => $tableInfo['name'],
            'primaryKeysNames' => $tableInfo['primaryKey'],
            'columns' => array_values($columns),
        ];

        if ($tableInfo['bucket']['backend'] === 'synapse') {
            $data['distribution'] = [
                'type' => $tableInfo['distributionType'],
                'distributionColumnsNames' => $tableInfo['distributionKey'],
            ];
            $data['index'] = [
                'type' => $tableInfo['indexType'],
                'indexColumnsNames' => $tableInfo['indexKey'],
            ];
        }

        try {
            $this->client->createTableDefinition(
                $tableInfo['bucket']['id'],
                $data,
            );
        } catch (ClientException $e) {
            if ($e->getCode() === 400
                && str_contains($e->getMessage(), 'Primary keys columns must be set nullable false')) {
                throw new StorageApiException(sprintf(
                    'Table "%s" cannot be restored because the primary key cannot be set on a nullable column.',
                    $tableInfo['name'],
                ));
            }
            throw $e;
        }
    }

    private function restoreTableColumnsMetadata(array $tableInfo, string $tableId, Metadata $metadataClient): void
    {
        $metadatas = [];
        if (isset($tableInfo['metadata']) && count($tableInfo['metadata'])) {
            foreach ($this->prepareMetadata($tableInfo['metadata']) as $provider => $metadata) {
                $metadatas[$provider]['table'] = $metadata;
            }
        }
        foreach ($tableInfo['columnMetadata'] ?? [] as $column => $columnMetadata) {
            foreach ($this->prepareMetadata($columnMetadata) as $provider => $metadata) {
                if ($metadata !== []) {
                    $metadatas[$provider]['columns'][$column] = $metadata;
                }
            }
        }

        /** @var array $metadata */
        foreach ($metadatas as $provider => $metadata) {
            if ($provider === 'storage') {
                continue;
            }
            $tableMetadataUpdateOptions = new TableMetadataUpdateOptions(
                $tableId,
                (string) $provider,
                $metadata['table'] ?? null,
                $metadata['columns'] ?? null,
            );

            $metadataClient->postTableMetadataWithColumns($tableMetadataUpdateOptions);
        }
    }

    private function prepareMetadata(array $rawMetadata): array
    {
        $result = [];
        foreach ($rawMetadata as $item) {
            $result[$item['provider']][] = [
                'key' => $item['key'],
                'value' => $item['value'],
            ];
        }
        return $result;
    }

    private function getDestinationBucketBackend(string $bucketId): string
    {
        $bucket = $this->client->getBucket($bucketId);
        return $bucket['backend'];
    }

    private function getBaseType(string $backend, string $originalType): ?string
    {
        switch ($backend) {
            case 'snowflake':
                return (new Snowflake($originalType))->getBasetype();
            case 'bigquery':
                return (new Bigquery($originalType))->getBasetype();
            default:
                return null;
        }
    }

    /**
     * @return array{type: string, length?: string}
     */
    private function getDefinitionForBasetype(string $backend, ?string $basetype): array
    {
        if ($basetype === null) {
            // Fallback to STRING type if basetype cannot be determined
            return ['type' => 'STRING'];
        }

        switch ($backend) {
            case 'snowflake':
                $definition = Snowflake::getDefinitionForBasetype($basetype);
                break;
            case 'bigquery':
                $definition = Bigquery::getDefinitionForBasetype($basetype);
                break;
            default:
                return ['type' => 'STRING'];
        }

        $result = ['type' => $definition->getType()];
        if ($definition->getLength() !== null) {
            $result['length'] = $definition->getLength();
        }
        return $result;
    }
}
