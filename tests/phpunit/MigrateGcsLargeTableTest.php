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
        $storageObject->expects($this->once())->method('downloadAsString')->willReturn($manifestJson);

        $bucket = $this->createMock(Bucket::class);
        $bucket->expects($this->once())->method('object')->willReturn($storageObject);

        $gcsClient = $this->createMock(GoogleStorageClient::class);
        $gcsClient->expects($this->once())->method('bucket')->willReturn($bucket);

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
        $sourceClient->expects($this->once())->method('getFile')->willReturn([
            'gcsPath' => ['bucket' => $gcsBucket, 'key' => 'path/to/file/'],
            'gcsCredentials' => [],
        ]);
        $sourceClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $sourceClient->expects($this->once())->method('getTokenString')->willReturn('source-token');
        return $sourceClient;
    }

    public function testDryRunSkipsMigration(): void
    {
        $sourceClient = $this->createMock(Client::class);
        $sourceClient->expects($this->never())->method('getFile');

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->never())->method('getTable');
        $targetClient->expects($this->never())->method('queueTableImport');

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
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->with($tableId)->willReturn(['primaryKey' => ['id']]);
        $targetClient->expects($this->once())->method('removeTablePrimaryKey')->with($tableId);
        $targetClient->expects($this->once())->method('createTablePrimaryKey')->with($tableId, ['id']);
        $targetClient->expects($this->once())->method('queueTableImport')->willReturn(999);
        $targetClient->expects($this->once())->method('getJob')->with(999)->willReturn(['status' => 'success']);

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
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->with($tableId)->willReturn(['primaryKey' => ['id']]);
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

    public function testMultipleChunksWithLimitedParallelism(): void
    {
        $tableId = 'in.c-test.my_table';

        $jobIds = [];
        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->exactly(3))
            ->method('queueTableImport')
            ->willReturnCallback(function () use (&$jobIds): int {
                $jobId = 100 + count($jobIds);
                $jobIds[] = $jobId;
                return $jobId;
            });
        $targetClient->expects($this->exactly(3))
            ->method('getJob')
            ->willReturn(['status' => 'success']);

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            maxParallelism: 2,
            chunkSize: 1,
        );

        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([
                ['url' => 'gs://test-bucket/path/slice1'],
                ['url' => 'gs://test-bucket/path/slice2'],
                ['url' => 'gs://test-bucket/path/slice3'],
            ]),
            $this->buildSuccessfulProcessFactory(),
        );
    }

    public function testMultipleChunkFailuresAreAggregated(): void
    {
        $tableId = 'in.c-test.my_table';

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->never())->method('queueTableImport');

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            maxParallelism: 2,
            chunkSize: 1,
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Failed 3 chunk(s).');

        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([
                ['url' => 'gs://test-bucket/path/slice1'],
                ['url' => 'gs://test-bucket/path/slice2'],
                ['url' => 'gs://test-bucket/path/slice3'],
            ]),
            $this->buildFailingProcessFactory(),
        );
    }

    public function testTableWithoutPrimaryKeySkipsPkCalls(): void
    {
        $tableId = 'in.c-test.my_table';

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->never())->method('removeTablePrimaryKey');
        $targetClient->expects($this->never())->method('createTablePrimaryKey');
        $targetClient->expects($this->once())->method('queueTableImport')->willReturn(999);
        $targetClient->expects($this->once())->method('getJob')->willReturn(['status' => 'success']);

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

    public function testFailedImportJobIsReportedWithChunkNumber(): void
    {
        $tableId = 'in.c-test.my_table';

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->once())->method('queueTableImport')->willReturn(777);
        $targetClient->expects($this->once())->method('getJob')->with(777)->willReturn([
            'status' => 'error',
            'error' => ['message' => 'CSV processing encountered too many errors'],
        ]);

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            chunkSize: 999,
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Failed 1 chunk(s). First: Chunk 1/1 import job 777 failed: '
            . 'CSV processing encountered too many errors (fileId: 42)',
        );

        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([['url' => 'gs://test-bucket/path/slice1']]),
            $this->buildSuccessfulProcessFactory(),
        );
    }

    public function testParallelImportsRunMultipleJobsConcurrently(): void
    {
        $tableId = 'in.c-test.my_table';

        $queuedJobs = [];
        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->exactly(3))
            ->method('queueTableImport')
            ->willReturnCallback(function () use (&$queuedJobs): int {
                $jobId = 200 + count($queuedJobs);
                $queuedJobs[] = $jobId;
                return $jobId;
            });
        // With parallelImports=3 all three jobs should be in-flight before any getJob succeeds.
        $targetClient->expects($this->exactly(3))
            ->method('getJob')
            ->willReturnCallback(function (int $jobId) use (&$queuedJobs): array {
                // All three jobs must have been queued before the first getJob poll resolves.
                self::assertCount(3, $queuedJobs, sprintf(
                    'getJob(%d) called while only %d jobs queued (expected all 3 to be queued first)',
                    $jobId,
                    count($queuedJobs),
                ));
                return ['status' => 'success'];
            });

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            maxParallelism: 3,
            chunkSize: 1,
            parallelImports: 3,
        );

        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([
                ['url' => 'gs://test-bucket/path/slice1'],
                ['url' => 'gs://test-bucket/path/slice2'],
                ['url' => 'gs://test-bucket/path/slice3'],
            ]),
            $this->buildSuccessfulProcessFactory(),
        );
    }

    public function testLastChunksProcessesOnlyTrailingChunks(): void
    {
        $tableId = 'in.c-test.my_table';

        $startedChunkNums = [];
        $processFactory = function (array $input) use (&$startedChunkNums): Process {
            $startedChunkNums[] = $input['chunkNum'];
            $process = $this->createMock(Process::class);
            $process->method('start');
            $process->method('isRunning')->willReturn(false);
            $process->method('isSuccessful')->willReturn(true);
            $process->method('getOutput')->willReturn(json_encode([
                'fileId' => '42',
                'logs' => [],
            ]));
            return $process;
        };

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        // 5 total chunks, lastChunks=2 → only chunks 4 and 5 are imported.
        $targetClient->expects($this->exactly(2))->method('queueTableImport')->willReturn(123);
        $targetClient->expects($this->exactly(2))->method('getJob')->willReturn(['status' => 'success']);

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            chunkSize: 1,
            lastChunks: 2,
        );

        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([
                ['url' => 'gs://test-bucket/path/slice1'],
                ['url' => 'gs://test-bucket/path/slice2'],
                ['url' => 'gs://test-bucket/path/slice3'],
                ['url' => 'gs://test-bucket/path/slice4'],
                ['url' => 'gs://test-bucket/path/slice5'],
            ]),
            $processFactory,
        );

        self::assertSame([4, 5], $startedChunkNums);
    }

    public function testLastChunksLargerThanTotalProcessesAll(): void
    {
        $tableId = 'in.c-test.my_table';

        $startedChunkNums = [];
        $processFactory = function (array $input) use (&$startedChunkNums): Process {
            $startedChunkNums[] = $input['chunkNum'];
            $process = $this->createMock(Process::class);
            $process->method('start');
            $process->method('isRunning')->willReturn(false);
            $process->method('isSuccessful')->willReturn(true);
            $process->method('getOutput')->willReturn(json_encode([
                'fileId' => '42',
                'logs' => [],
            ]));
            return $process;
        };

        $targetClient = $this->createMock(Client::class);
        $targetClient->expects($this->once())->method('getApiUrl')->willReturn('https://connection.keboola.com');
        $targetClient->expects($this->once())->method('getTokenString')->willReturn('target-token');
        $targetClient->expects($this->once())->method('getTable')->willReturn(['primaryKey' => []]);
        $targetClient->expects($this->exactly(2))->method('queueTableImport')->willReturn(123);
        $targetClient->expects($this->exactly(2))->method('getJob')->willReturn(['status' => 'success']);

        $migrator = new MigrateGcsLargeTable(
            $this->buildSourceClient(),
            $targetClient,
            $this->createMock(LoggerInterface::class),
            chunkSize: 1,
            lastChunks: 99,
        );

        $migrator->migrate(
            123,
            ['id' => $tableId, 'name' => 'my_table', 'columns' => ['id', 'name']],
            false,
            $this->buildGcsClientFactory([
                ['url' => 'gs://test-bucket/path/slice1'],
                ['url' => 'gs://test-bucket/path/slice2'],
            ]),
            $processFactory,
        );

        self::assertSame([1, 2], $startedChunkNums);
    }
}
