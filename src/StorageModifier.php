<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception as StorageApiException;
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
        list($bucketStage, $bucketName) = explode('.', $schemaName);
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
    }

    private function createNonTypedTable(array $tableInfo): void
    {
        $headerFile = $this->tmp->createFile(sprintf('%s.header.csv', $tableInfo['id']));
        $headerFile = new CsvFile($headerFile->getPathname());
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
        $columns = [];
        foreach ($tableInfo['columns'] as $column) {
            $columns[$column] = [];
        }
        foreach ($tableInfo['columnMetadata'] ?? [] as $columnName => $column) {
            $columnMetadata = [];
            foreach ($column as $metadata) {
                if ($metadata['provider'] !== 'storage') {
                    continue;
                }
                $columnMetadata[$metadata['key']] = $metadata['value'];
            }

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
}