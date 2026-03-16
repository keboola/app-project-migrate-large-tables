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
    private function buildConfig(array $migrateTables = []): Config
    {
        $params = [
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
        ];
        if ($migrateTables !== []) {
            $params['tables'] = $migrateTables;
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

        $sourceClient->method('getTable')->willReturn($tableInfo);
        $sourceClient->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);
        $sourceClient->method('getFile')->willReturn($fileInfo);
        $sourceClient->method('downloadFile');

        $targetClient->method('bucketExists')->willReturn(true);
        $targetClient->method('tableExists')->willReturn(false);
        $targetClient->method('getBucket')->willReturn(['backend' => 'bigquery']);
        $targetClient->method('uploadFile')->willReturn(456);
        $targetClient->method('writeTableAsyncDirect');
        $targetClient->method('createTableDefinition');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->arrayHasKey('timezone'),
            );

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testExportTimezoneValueIsUtcForSnowflakeToBigquery(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('snowflake');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->method('getTable')->willReturn($tableInfo);
        $sourceClient->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);
        $sourceClient->method('getFile')->willReturn($fileInfo);
        $sourceClient->method('downloadFile');

        $targetClient->method('bucketExists')->willReturn(true);
        $targetClient->method('tableExists')->willReturn(false);
        $targetClient->method('getBucket')->willReturn(['backend' => 'bigquery']);
        $targetClient->method('uploadFile')->willReturn(456);
        $targetClient->method('writeTableAsyncDirect');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->callback(fn(array $options) => isset($options['timezone']) && $options['timezone'] === 'UTC'),
            );

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testExportDoesNotPassTimezoneForSnowflakeToSnowflake(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('snowflake');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->method('getTable')->willReturn($tableInfo);
        $sourceClient->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);
        $sourceClient->method('getFile')->willReturn($fileInfo);
        $sourceClient->method('downloadFile');

        $targetClient->method('bucketExists')->willReturn(true);
        $targetClient->method('tableExists')->willReturn(false);
        $targetClient->method('getBucket')->willReturn(['backend' => 'snowflake']);
        $targetClient->method('uploadFile')->willReturn(456);
        $targetClient->method('writeTableAsyncDirect');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->callback(fn(array $options) => !isset($options['timezone'])),
            );

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testExportDoesNotPassTimezoneForBigquerySource(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo = $this->buildTableInfo('bigquery');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->method('getTable')->willReturn($tableInfo);
        $sourceClient->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);
        $sourceClient->method('getFile')->willReturn($fileInfo);
        $sourceClient->method('downloadFile');

        $targetClient->method('bucketExists')->willReturn(true);
        $targetClient->method('tableExists')->willReturn(false);
        $targetClient->method('uploadFile')->willReturn(456);
        $targetClient->method('writeTableAsyncDirect');

        $sourceClient->expects($this->once())
            ->method('exportTableAsync')
            ->with(
                $tableInfo['id'],
                $this->callback(fn(array $options) => !isset($options['timezone'])),
            );

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table']));
    }

    public function testDestinationBucketBackendIsCachedAcrossMultipleTables(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $targetClient = $this->createMock(Client::class);

        $tableInfo1 = $this->buildTableInfo('snowflake', 'in.c-test.table1');
        $tableInfo2 = $this->buildTableInfo('snowflake', 'in.c-test.table2');
        $fileInfo = $this->buildFileInfo();

        $sourceClient->method('getTable')->willReturnOnConsecutiveCalls($tableInfo1, $tableInfo2);
        $sourceClient->method('exportTableAsync')->willReturn(['file' => ['id' => 123]]);
        $sourceClient->method('getFile')->willReturn($fileInfo);
        $sourceClient->method('downloadFile');

        $targetClient->method('bucketExists')->willReturn(true);
        $targetClient->method('tableExists')->willReturn(false);
        // getBucket should be called only once due to caching
        $targetClient->expects($this->once())
            ->method('getBucket')
            ->willReturn(['backend' => 'bigquery']);
        $targetClient->method('uploadFile')->willReturn(456);
        $targetClient->method('writeTableAsyncDirect');

        $migrate = new SapiMigrate($sourceClient, $targetClient, new NullLogger());
        $migrate->migrate($this->buildConfig(['in.c-test.table1', 'in.c-test.table2']));
    }
}
