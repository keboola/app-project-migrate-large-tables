<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\BaseType;
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

    /** @var array<string, string> */
    private array $bucketBackendCache = [];

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
        foreach ($tableInfo['definition']['columns'] as $columnDef) {
            $columnName = $columnDef['name'];
            $basetype = $columnDef['basetype'] ?? null;
            $nullable = (bool) $columnDef['definition']['nullable'];

            if ($sourceBackend !== $destinationBackend) {
                $definition = $this->buildCrossBackendDefinition($destinationBackend, $basetype, $nullable);
            } else {
                $definition = [
                    'type' => $columnDef['definition']['type'],
                    'nullable' => $nullable,
                ];
                if (isset($columnDef['definition']['length'])) {
                    $definition['length'] = $columnDef['definition']['length'];
                }
                if (isset($columnDef['definition']['default'])) {
                    $definition['default'] = $columnDef['definition']['default'];
                }
            }

            $columns[] = [
                'name' => $columnName,
                'definition' => $definition,
                'basetype' => $basetype,
            ];
        }

        $data = [
            'name' => $tableInfo['name'],
            'primaryKeysNames' => $tableInfo['primaryKey'],
            'columns' => $columns,
        ];

        if ($sourceBackend === 'synapse') {
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

    private function buildCrossBackendDefinition(string $destinationBackend, ?string $basetype, bool $nullable): array
    {
        $effectiveBasetype = ($basetype !== null && BaseType::isValid(strtoupper($basetype)))
            ? strtoupper($basetype)
            : BaseType::STRING;

        $nativeType = match ($destinationBackend) {
            'bigquery' => Bigquery::getTypeByBasetype($effectiveBasetype),
            'snowflake' => Snowflake::getTypeByBasetype($effectiveBasetype),
            default => $effectiveBasetype,
        };

        return [
            'type' => $nativeType,
            'nullable' => $nullable,
        ];
    }

    private function getDestinationBucketBackend(string $bucketId): string
    {
        if (!array_key_exists($bucketId, $this->bucketBackendCache)) {
            $bucket = $this->client->getBucket($bucketId);
            $this->bucketBackendCache[$bucketId] = $bucket['backend'];
        }

        return $this->bucketBackendCache[$bucketId];
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
}
