<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\Component\UserException;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class StorageModifierTest extends TestCase
{
    private function buildColumnDef(
        string $name,
        string $type,
        ?string $basetype,
        bool $nullable = true,
        ?string $length = null,
        ?string $default = null,
    ): array {
        $definition = [
            'type' => $type,
            'nullable' => $nullable,
        ];
        if ($length !== null) {
            $definition['length'] = $length;
        }
        if ($default !== null) {
            $definition['default'] = $default;
        }

        return [
            'name' => $name,
            'basetype' => $basetype,
            'definition' => $definition,
        ];
    }

    private function buildTableInfo(
        string $sourceBackend,
        string $bucketId,
        array $columns,
        array $primaryKey = [],
    ): array {
        return [
            'id' => sprintf('%s.my_table', $bucketId),
            'name' => 'my_table',
            'isTyped' => true,
            'primaryKey' => $primaryKey,
            'bucket' => [
                'id' => $bucketId,
                'backend' => $sourceBackend,
            ],
            'definition' => [
                'columns' => $columns,
            ],
            'metadata' => [],
            'columnMetadata' => [],
        ];
    }

    public function testCreateTypedTableSameBackendPreservesNativeTypeWithLengthAndDefault(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->expects($this->once())
            ->method('getBucket')
            ->with($bucketId)
            ->willReturn(['backend' => 'snowflake']);

        $capturedData = null;
        $client->expects($this->once())
            ->method('createTableDefinition')
            ->with($bucketId, $this->callback(function (array $data) use (&$capturedData) {
                $capturedData = $data;
                return true;
            }));

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('id', 'INTEGER', 'INTEGER', false),
                $this->buildColumnDef('name', 'VARCHAR', 'STRING', true, '255', 'unknown'),
                $this->buildColumnDef('amount', 'NUMBER', 'NUMERIC', true, '18,2'),
            ],
        ));

        $this->assertNotNull($capturedData);
        $this->assertSame([
            [
                'name' => 'id',
                'definition' => ['type' => 'INTEGER', 'nullable' => false],
                'basetype' => 'INTEGER',
            ],
            [
                'name' => 'name',
                'definition' => ['type' => 'VARCHAR', 'nullable' => true, 'length' => '255', 'default' => 'unknown'],
                'basetype' => 'STRING',
            ],
            [
                'name' => 'amount',
                'definition' => ['type' => 'NUMBER', 'nullable' => true, 'length' => '18,2'],
                'basetype' => 'NUMERIC',
            ],
        ], $capturedData['columns']);
    }

    public function testCreateTypedTableCrossBackendSnowflakeToBigquery(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->with($bucketId)
            ->willReturn(['backend' => 'bigquery']);

        $capturedData = null;
        $client->expects($this->once())
            ->method('createTableDefinition')
            ->with($bucketId, $this->callback(function (array $data) use (&$capturedData) {
                $capturedData = $data;
                return true;
            }));

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('id', 'INTEGER', 'INTEGER', false),
                $this->buildColumnDef('name', 'VARCHAR', 'STRING', true, '255'),
                $this->buildColumnDef('price', 'FLOAT', 'FLOAT', true),
                $this->buildColumnDef('active', 'BOOLEAN', 'BOOLEAN', true),
                $this->buildColumnDef('created', 'TIMESTAMP', 'TIMESTAMP', true),
                $this->buildColumnDef('date_col', 'DATE', 'DATE', true),
                $this->buildColumnDef('amount', 'NUMBER', 'NUMERIC', true),
            ],
        ));

        $expectedColumns = [
            ['name' => 'id', 'definition' => ['type' => 'INT64', 'nullable' => false], 'basetype' => 'INTEGER'],
            ['name' => 'name', 'definition' => ['type' => 'STRING', 'nullable' => true], 'basetype' => 'STRING'],
            ['name' => 'price', 'definition' => ['type' => 'FLOAT64', 'nullable' => true], 'basetype' => 'FLOAT'],
            ['name' => 'active', 'definition' => ['type' => 'BOOL', 'nullable' => true], 'basetype' => 'BOOLEAN'],
            [
                'name' => 'created',
                'definition' => ['type' => 'TIMESTAMP', 'nullable' => true],
                'basetype' => 'TIMESTAMP',
            ],
            ['name' => 'date_col', 'definition' => ['type' => 'DATE', 'nullable' => true], 'basetype' => 'DATE'],
            ['name' => 'amount', 'definition' => ['type' => 'NUMERIC', 'nullable' => true], 'basetype' => 'NUMERIC'],
        ];

        $this->assertNotNull($capturedData);
        $this->assertSame($expectedColumns, $capturedData['columns']);
    }

    public function testCreateTypedTableCrossBackendBigqueryToSnowflake(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->with($bucketId)
            ->willReturn(['backend' => 'snowflake']);

        $capturedData = null;
        $client->expects($this->once())
            ->method('createTableDefinition')
            ->with($bucketId, $this->callback(function (array $data) use (&$capturedData) {
                $capturedData = $data;
                return true;
            }));

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'bigquery',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('id', 'INT64', 'INTEGER', false),
                $this->buildColumnDef('name', 'STRING', 'STRING', true),
                $this->buildColumnDef('price', 'FLOAT64', 'FLOAT', true),
                $this->buildColumnDef('active', 'BOOL', 'BOOLEAN', true),
                $this->buildColumnDef('created', 'TIMESTAMP', 'TIMESTAMP', true),
                $this->buildColumnDef('amount', 'NUMERIC', 'NUMERIC', true),
            ],
        ));

        $expectedColumns = [
            ['name' => 'id', 'definition' => ['type' => 'INTEGER', 'nullable' => false], 'basetype' => 'INTEGER'],
            ['name' => 'name', 'definition' => ['type' => 'VARCHAR', 'nullable' => true], 'basetype' => 'STRING'],
            ['name' => 'price', 'definition' => ['type' => 'FLOAT', 'nullable' => true], 'basetype' => 'FLOAT'],
            [
                'name' => 'active',
                'definition' => ['type' => 'BOOLEAN', 'nullable' => true],
                'basetype' => 'BOOLEAN',
            ],
            [
                'name' => 'created',
                'definition' => ['type' => 'TIMESTAMP', 'nullable' => true],
                'basetype' => 'TIMESTAMP',
            ],
            ['name' => 'amount', 'definition' => ['type' => 'NUMBER', 'nullable' => true], 'basetype' => 'NUMERIC'],
        ];

        $this->assertNotNull($capturedData);
        $this->assertSame($expectedColumns, $capturedData['columns']);
    }

    public function testCreateTypedTableCrossBackendNullBasetypeFallsBackToString(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->willReturn(['backend' => 'bigquery']);

        $capturedData = null;
        $client->method('createTableDefinition')
            ->willReturnCallback(function (string $id, array $data) use (&$capturedData): void {
                $capturedData = $data;
            });

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('col', 'VARIANT', null, true),
            ],
        ));

        $this->assertNotNull($capturedData);
        $this->assertSame('STRING', $capturedData['columns'][0]['definition']['type']);
    }

    public function testCreateTypedTableCrossBackendUnknownBasetypeFallsBackToString(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->willReturn(['backend' => 'snowflake']);

        $capturedData = null;
        $client->method('createTableDefinition')
            ->willReturnCallback(function (string $id, array $data) use (&$capturedData): void {
                $capturedData = $data;
            });

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'bigquery',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('col', 'JSON', 'SOME_UNKNOWN_TYPE', true),
            ],
        ));

        $this->assertNotNull($capturedData);
        $this->assertSame('VARCHAR', $capturedData['columns'][0]['definition']['type']);
    }

    public function testCreateTypedTableCrossBackendDoesNotCopyLengthOrDefault(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->willReturn(['backend' => 'bigquery']);

        $capturedData = null;
        $client->method('createTableDefinition')
            ->willReturnCallback(function (string $id, array $data) use (&$capturedData): void {
                $capturedData = $data;
            });

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('name', 'VARCHAR', 'STRING', true, '255', 'foo'),
            ],
        ));

        $this->assertNotNull($capturedData);
        $columnDef = $capturedData['columns'][0]['definition'];
        $this->assertArrayNotHasKey('length', $columnDef);
        $this->assertArrayNotHasKey('default', $columnDef);
    }

    public function testGetDestinationBucketBackendIsCachedAcrossMultipleTables(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        // getBucket should only be called once even when creating multiple tables in the same bucket
        $client->expects($this->once())
            ->method('getBucket')
            ->with($bucketId)
            ->willReturn(['backend' => 'snowflake']);

        $client->method('createTableDefinition');

        $modifier = new StorageModifier($client);

        $tableInfo = $this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [$this->buildColumnDef('id', 'INTEGER', 'INTEGER', false)],
        );

        $tableInfo2 = array_merge($tableInfo, ['name' => 'my_table_2', 'id' => $bucketId . '.my_table_2']);

        $modifier->createTable($tableInfo);
        $modifier->createTable($tableInfo2);
    }

    public function testCreateTypedTableWithPrimaryKey(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->willReturn(['backend' => 'snowflake']);

        $capturedData = null;
        $client->method('createTableDefinition')
            ->willReturnCallback(function (string $id, array $data) use (&$capturedData): void {
                $capturedData = $data;
            });

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('id', 'INTEGER', 'INTEGER', false),
                $this->buildColumnDef('name', 'VARCHAR', 'STRING', true),
            ],
            primaryKey: ['id'],
        ));

        $this->assertNotNull($capturedData);
        $this->assertSame(['id'], $capturedData['primaryKeysNames']);
        $this->assertSame('my_table', $capturedData['name']);
    }

    public function testCreateTypedTableSnowflakeToBigqueryThrowsForNumericScaleOver9(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->with($bucketId)
            ->willReturn(['backend' => 'bigquery']);

        $client->expects($this->never())
            ->method('createTableDefinition');

        $modifier = new StorageModifier($client);

        $this->expectException(UserException::class);
        $this->expectExceptionMessageMatches('/Column "amount"/');
        $this->expectExceptionMessageMatches('/NUMBER\(38,12\)/');

        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('amount', 'NUMBER', 'NUMERIC', true, '38,12'),
            ],
        ));
    }

    public function testCreateTypedTableSnowflakeToBigqueryAllowsNumericScaleOf9(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->with($bucketId)
            ->willReturn(['backend' => 'bigquery']);

        $client->expects($this->once())
            ->method('createTableDefinition');

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('amount', 'NUMBER', 'NUMERIC', true, '38,9'),
            ],
        ));
    }

    public function testCreateTypedTableWithUnknownDestinationBackendUsesBasetypeAsType(): void
    {
        $bucketId = 'in.c-test';

        $client = $this->createMock(Client::class);
        $client->method('getBucket')
            ->willReturn(['backend' => 'exasol']);

        $capturedData = null;
        $client->method('createTableDefinition')
            ->willReturnCallback(function (string $id, array $data) use (&$capturedData): void {
                $capturedData = $data;
            });

        $modifier = new StorageModifier($client);
        $modifier->createTable($this->buildTableInfo(
            sourceBackend: 'snowflake',
            bucketId: $bucketId,
            columns: [
                $this->buildColumnDef('id', 'INTEGER', 'INTEGER', false),
                $this->buildColumnDef('name', 'VARCHAR', 'STRING', true),
            ],
        ));

        $this->assertNotNull($capturedData);
        // For unknown backends, basetype is used directly as type
        $this->assertSame('INTEGER', $capturedData['columns'][0]['definition']['type']);
        $this->assertSame('STRING', $capturedData['columns'][1]['definition']['type']);
    }
}
