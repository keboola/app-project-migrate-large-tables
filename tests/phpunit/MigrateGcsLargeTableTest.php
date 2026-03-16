<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use Google\Cloud\Storage\StorageObject;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate\MigrateGcsLargeTable;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Symfony\Component\Process\Process;

class MigrateGcsLargeTableTest extends TestCase
{
    private function buildGcsClientFactory(array $manifestEntries): callable
    {
        $manifestJson = json_encode(['entries' => $manifestEntries]);

        $storageObject = $this->createMock(StorageObject::class);
        $storageObject->method('downloadAsString')->willReturn($manifestJson);

        $bucket = $this->createMock(Bucket::class);
        $bucket->method('object')->willReturn($storageObject);

        $gcsClient = $this->createMock(GoogleStorageClient::class);
        $gcsClient->method('bucket')->willReturn($bucket);

        return fn(int $fileId) => $gcsClient;
    }

    private function buildSuccessfulProcessFactory(string $uploadedFileId = '42'): callable
    {
        return function (array $input) use ($uploadedFileId): Process {
            $process = $this->createMock(Process::class);
            $process->method('start');
            $process->method('isRunning')->willReturn(false);
            $process->method('isSuccessful')->willReturn(true);
            $process->method('getOutput')->willReturn(json_encode([
                'fileId' => $uploadedFileId,
                'logs' => [sprintf('Chunk %d/%d: done', $input['chunkNum'], $input['totalChunks'])],
            ]));
            return $process;
        };
    }

    private function buildFailingProcessFactory(): callable
    {
        return function (array $input): Process {
            $process = $this->createMock(Process::class);
            $process->method('start');
            $process->method('isRunning')->willReturn(false);
            $process->method('isSuccessful')->willReturn(false);
            $process->method('getExitCode')->willReturn(1);
            $process->method('getErrorOutput')->willReturn('Something went wrong');
            $process->method('stop');
            return $process;
        };
    }

    private function buildSourceClient(string $gcsBucket = 'test-bucket'): Client
    {
        $sourceClient = $this->createMock(Client::class);
        $sourceClient->method('getFile')->willReturn([
            'gcsPath' => ['bucket' => $gcsBucket, 'key' => 'path/to/file/'],
            'gcsCredentials' => [],
        ]);
        $sourceClient->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $sourceClient->method('getTokenString')->willReturn('source-token');
        return $sourceClient;
    }

    public function testDryRunSkipsMigration(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $sourceClient->expects($this->never())->method('getFile');

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->never())->method('getTable');
        $targetClient->expects($this->never())->method('writeTableAsyncDirect');

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('info')
            ->with($this->stringContains('[dry-run] Migrate table in.c-test.my_table'));

        $migrator = new MigrateGcsLargeTable($sourceClient, $targetClient, $logger, dryRun: true);
        $migrator->migrate(123, ['id' => 'in.c-test.my_table'], false);
    }

    public function testPrimaryKeyIsRemovedBeforeAndRestoredAfterMigration(): void
    {
        $tableId = 'in.c-test.my_table';

        $targetClient = $this->createMock(Client::class);
        $targetClient->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->method('getTokenString')->willReturn('target-token');
        $targetClient->method('getTable')->with($tableId)->willReturn(['primaryKey' => ['id']]);
        $targetClient->expects($this->once())->method('removeTablePrimaryKey')->with($tableId);
        $targetClient->expects($this->once())->method('createTablePrimaryKey')->with($tableId, ['id']);
        $targetClient->expects($this->once())->method('writeTableAsyncDirect');

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            chunkSize: 999,
        );
        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([['url' => 'gs://test-bucket/path/slice1']]),
            $this->buildSuccessfulProcessFactory(),
        );
    }

    public function testPrimaryKeyIsRestoredEvenWhenWorkerFails(): void
    {
        $tableId = 'in.c-test.my_table';

        $targetClient = $this->createMock(Client::class);
        $targetClient->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->method('getTokenString')->willReturn('target-token');
        $targetClient->method('getTable')->with($tableId)->willReturn(['primaryKey' => ['id']]);
        $targetClient->expects($this->once())->method('removeTablePrimaryKey')->with($tableId);
        $targetClient->expects($this->once())->method('createTablePrimaryKey')->with($tableId, ['id']);

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            chunkSize: 999,
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Failed 1 chunk(s). First: Chunk 1/1 worker exited with code 1: Something went wrong',
        );
        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([['url' => 'gs://test-bucket/path/slice1']]),
            $this->buildFailingProcessFactory(),
        );
    }

    public function testTableWithoutPrimaryKeySkipsPkCalls(): void
    {
        $tableId = 'in.c-test.my_table';

        $targetClient = $this->createMock(Client::class);
        $targetClient->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->method('getTokenString')->willReturn('target-token');
        $targetClient->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->never())->method('removeTablePrimaryKey');
        $targetClient->expects($this->never())->method('createTablePrimaryKey');
        $targetClient->method('writeTableAsyncDirect');

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            chunkSize: 999,
        );
        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([['url' => 'gs://test-bucket/path/slice1']]),
            $this->buildSuccessfulProcessFactory(),
        );
    }
}
