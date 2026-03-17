<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\Configuration\ConfigDefinition;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class SapiMigrateTest extends TestCase
{
    private function buildConfig(array $migrateTables = [], bool $forcePrimaryKeyNotNull = false): Config
    {
        $params = [
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
        ];
        if ($migrateTables !== []) {
            $params['tables'] = $migrateTables;
        }
        if ($forcePrimaryKeyNotNull) {
            $params['forcePrimaryKeyNotNull'] = true;
        }

        return new Config(['parameters' => $params], new ConfigDefinition());
    }

    private function buildTableInfo(string $sourceBackend, string $tableId = 'in.c-test.table'): array
    {
        return [
            'id' => $tableId,
            'name' => 'table',
            'columns' => ['id', 'name'],
            'isAlias' => false,
            'primaryKey' => [],
            'isTyped' => false,
            'bucket' => [
                'id' => 'in.c-test',
                'stage' => 'in',
                'backend' => $sourceBackend,
            ],
        ];
    }

    private function buildFileInfo(bool $isSliced = false, string $provider = 'aws'): array
    {
        return [
            'id' => 123,
            'name' => 'table.csv',
            'provider' => $provider,
            'isSliced' => $isSliced,
            'sizeBytes' => 1000,
        ];
    }

    public function testExportPassesTimezoneUtcForSnowflakeToBigquery(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('snowflake');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->expects($this->once())->method('getTable')->with($tableInfo['id'])->willReturn($tableInfo);
        $sourceClient->expects($this->once())->method('getFile')->with(123)->willReturn($fileInfo);
        $sourceClient->expects($this->once())->method('downloadFile');

        $targetClient->expects($this->once())->method('bucketExists')->with('in.c-test')->willReturn(true);
        $targetClient->expects($this->once())->method('tableExists')->with($tableInfo['id'])->willReturn(false);
        $targetClient->expects($this->once())->method('getBucket')
            ->with('in.c-test')->willReturn(['backend' => 'bigquery']);
        $targetClient->expects($this->once())->method('uploadFile')->willReturn(456);
        $targetClient->expects($this->once())->method('writeTableAsyncDirect');
        $targetClient->expects($this->never())->method('createTableDefinition');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->arrayHasKey('timezone'),
            )
            ->willReturn(['file' => ['id' => 123]]);

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testExportTimezoneValueIsUtcForSnowflakeToBigquery(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('snowflake');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->expects($this->once())->method('getTable')->with($tableInfo['id'])->willReturn($tableInfo);
        $sourceClient->expects($this->once())->method('getFile')->with(123)->willReturn($fileInfo);
        $sourceClient->expects($this->once())->method('downloadFile');

        $targetClient->expects($this->once())->method('bucketExists')->with('in.c-test')->willReturn(true);
        $targetClient->expects($this->once())->method('tableExists')->with($tableInfo['id'])->willReturn(false);
        $targetClient->expects($this->once())->method('getBucket')
            ->with('in.c-test')->willReturn(['backend' => 'bigquery']);
        $targetClient->expects($this->once())->method('uploadFile')->willReturn(456);
        $targetClient->expects($this->once())->method('writeTableAsyncDirect');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->callback(fn(array $options) => isset($options['timezone']) && $options['timezone'] === 'UTC'),
            )
            ->willReturn(['file' => ['id' => 123]]);

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testExportDoesNotPassTimezoneForSnowflakeToSnowflake(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('snowflake');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->expects($this->once())->method('getTable')->with($tableInfo['id'])->willReturn($tableInfo);
        $sourceClient->expects($this->once())->method('getFile')->with(123)->willReturn($fileInfo);
        $sourceClient->expects($this->once())->method('downloadFile');

        $targetClient->expects($this->once())->method('bucketExists')->with('in.c-test')->willReturn(true);
        $targetClient->expects($this->once())->method('tableExists')->with($tableInfo['id'])->willReturn(false);
        $targetClient->expects($this->once())->method('getBucket')
            ->with('in.c-test')->willReturn(['backend' => 'snowflake']);
        $targetClient->expects($this->once())->method('uploadFile')->willReturn(456);
        $targetClient->expects($this->once())->method('writeTableAsyncDirect');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->callback(fn(array $options) => !isset($options['timezone'])),
            )
            ->willReturn(['file' => ['id' => 123]]);

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testExportDoesNotPassTimezoneForBigquerySource(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('bigquery');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->expects($this->once())->method('getTable')->with($tableInfo['id'])->willReturn($tableInfo);
        $sourceClient->expects($this->once())->method('getFile')->with(123)->willReturn($fileInfo);
        $sourceClient->expects($this->once())->method('downloadFile');

        $targetClient->expects($this->once())->method('bucketExists')->with('in.c-test')->willReturn(true);
        $targetClient->expects($this->once())->method('tableExists')->with($tableInfo['id'])->willReturn(false);
        $targetClient->expects($this->never())->method('getBucket');
        $targetClient->expects($this->once())->method('uploadFile')->willReturn(456);
        $targetClient->expects($this->once())->method('writeTableAsyncDirect');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->callback(fn(array $options) => !isset($options['timezone'])),
            )
            ->willReturn(['file' => ['id' => 123]]);

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testForcePrimaryKeyNotNullIsPassedToTableCreation(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = array_merge($this->buildTableInfo('snowflake'), [
            'isTyped' => true,
            'primaryKey' => ['id'],
            'definition' => [
                'columns' => [
                    ['name' => 'id', 'basetype' => 'INTEGER', 'definition' => [
                        'type' => 'INTEGER', 'nullable' => true,
                    ]],
                    ['name' => 'name', 'basetype' => 'STRING', 'definition' => [
                        'type' => 'VARCHAR', 'nullable' => true,
                    ]],
                ],
            ],
            'metadata' => [],
            'columnMetadata' => [],
        ]);
        $fileInfo = $this->buildFileInfo();

        $sourceClient->method('getTable')->willReturn($tableInfo);
        $sourceClient->method('getFile')->willReturn($fileInfo);
        $sourceClient->method('downloadFile');
        $sourceClient->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);

        $targetClient->method('bucketExists')->willReturn(true);
        $targetClient->method('tableExists')->willReturn(false);
        $targetClient->method('getBucket')->willReturn(['backend' => 'snowflake']);
        $targetClient->method('uploadFile')->willReturn(456);
        $targetClient->method('writeTableAsyncDirect');

        $capturedData = null;
        $targetClient->expects($this->once())
            ->method('createTableDefinition')
            ->willReturnCallback(function (string $id, array $data) use (&$capturedData): void {
                $capturedData = $data;
            });

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table'], forcePrimaryKeyNotNull: true));

        self::assertNotNull($capturedData);
        self::assertFalse($capturedData['columns'][0]['definition']['nullable'], 'PK column must be not nullable');
        self::assertTrue($capturedData['columns'][1]['definition']['nullable'], 'Non-PK column must stay nullable');
    }

    public function testDestinationBucketBackendIsCachedAcrossMultipleTables(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo1 = $this->buildTableInfo('snowflake', 'in.c-test.table1');
        $tableInfo2 = $this->buildTableInfo('snowflake', 'in.c-test.table2');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->expects($this->exactly(2))->method('getTable')
            ->willReturnOnConsecutiveCalls($tableInfo1, $tableInfo2);
        $sourceClient->expects($this->exactly(2))->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);
        $sourceClient->expects($this->exactly(2))->method('getFile')->willReturn($fileInfo);
        $sourceClient->expects($this->exactly(2))->method('downloadFile');

        $targetClient->expects($this->exactly(2))->method('bucketExists')->willReturn(true);
        $targetClient->expects($this->exactly(2))->method('tableExists')->willReturn(false);
        // getBucket should be called only once due to caching
        $targetClient->expects($this->once())
            ->method('getBucket')
            ->with('in.c-test')
            ->willReturn(['backend' => 'bigquery']);
        $targetClient->expects($this->exactly(2))->method('uploadFile')->willReturn(456);
        $targetClient->expects($this->exactly(2))->method('writeTableAsyncDirect');

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table1', 'in.c-test.table2']));
    }
}
