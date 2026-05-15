<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use GuzzleHttp\Utils;
use JsonException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Symfony\Component\Process\Process;
use Throwable;

class MigrateGcsLargeTable
{
    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
        private readonly int $maxParallelism = 3,
        private readonly int $chunkSize = 150,
        private readonly int $parallelImports = 1,
        private readonly int $lastChunks = 0,
    ) {
    }

    public function migrate(
        int $fileId,
        array $tableInfo,
        bool $preserveTimestamp,
        ?callable $gcsClientFactory = null,
        ?callable $processFactory = null,
    ): void {
        if ($this->dryRun === true) {
            $this->logger->info(sprintf('[dry-run] Migrate table %s', $tableInfo['id']));
            return;
        }

        $gcsClientFactory ??= fn(int $fileId) => $this->getGcsClient($fileId);
        $processFactory ??= fn(array $input) => new Process(
            [PHP_BINARY, __DIR__ . '/../../worker-chunk.php'],
            null,
            null,
            json_encode($input),
            null,
        );

        $fileInfo = $this->sourceClient->getFile(
            $fileId,
            (new GetFileOptions())->setFederationToken(true),
        );

        $bucket = $fileInfo['gcsPath']['bucket'];
        $gcsClient = $gcsClientFactory($fileId);
        $retBucket = $gcsClient->bucket($bucket);
        $manifestObject = $retBucket->object($fileInfo['gcsPath']['key'] . 'manifest')->downloadAsString();

        /** @var array{"entries": array<array{url: string}>} $manifest */
        $manifest = Utils::jsonDecode($manifestObject, true);
        $chunks = array_chunk((array) $manifest['entries'], max(1, $this->chunkSize));

        $totalChunks = count($chunks);
        $startChunkIndex = 0;
        if ($this->lastChunks > 0 && $this->lastChunks < $totalChunks) {
            $startChunkIndex = $totalChunks - $this->lastChunks;
            $this->logger->info(sprintf(
                'lastChunks=%d set: skipping chunks 1-%d, processing only chunks %d-%d',
                $this->lastChunks,
                $startChunkIndex,
                $startChunkIndex + 1,
                $totalChunks,
            ));
        }

        $this->logger->info(sprintf(
            'Processing table %s: %d chunks (worker parallelism %d, import parallelism %d)',
            $tableInfo['id'],
            $totalChunks - $startChunkIndex,
            $this->maxParallelism,
            $this->parallelImports,
        ));

        $sourceApiUrl = $this->sourceClient->getApiUrl();
        $sourceToken = $this->sourceClient->getTokenString();
        $targetApiUrl = $this->targetClient->getApiUrl();
        $targetToken = $this->targetClient->getTokenString();

        $targetTableInfo = $this->targetClient->getTable($tableInfo['id']);
        $primaryKey = $targetTableInfo['primaryKey'] ?? [];
        if (!empty($primaryKey)) {
            $this->logger->info(sprintf(
                'Removing primary key [%s] from %s before import',
                implode(', ', $primaryKey),
                $tableInfo['id'],
            ));
            $this->targetClient->removeTablePrimaryKey($tableInfo['id']);
        }

        /** @var array<int, array{process: Process, chunkNum: int}> $runningProcesses */
        $runningProcesses = [];
        /** @var array<array{fileId: string, chunkNum: int}> $writeQueue */
        $writeQueue = [];
        /** @var array<int, array{jobId: int, chunkNum: int, fileId: string}> $inFlightImports */
        $inFlightImports = [];
        $errors = [];
        $chunkIndex = $startChunkIndex;

        try {
            while ($chunkIndex < $totalChunks
                || !empty($runningProcesses)
                || !empty($writeQueue)
                || !empty($inFlightImports)
            ) {
                $didSomething = false;

                // --- Phase 1a: collect finished workers (free slots before starting new ones) ---
                foreach ($runningProcesses as $key => $item) {
                    if (!$item['process']->isRunning()) {
                        unset($runningProcesses[$key]);
                        $didSomething = true;
                        if (!$item['process']->isSuccessful()) {
                            $errors['worker-' . $key] = new RuntimeException(sprintf(
                                'Chunk %d/%d worker exited with code %d: %s',
                                $item['chunkNum'],
                                $totalChunks,
                                $item['process']->getExitCode(),
                                trim($item['process']->getErrorOutput()),
                            ));
                            continue;
                        }
                        try {
                            /** @var array{logs: string[], fileId: string} $result */
                            $result = json_decode($item['process']->getOutput(), true, 512, JSON_THROW_ON_ERROR);
                        } catch (JsonException $e) {
                            $errors['worker-' . $key] = new RuntimeException(sprintf(
                                'Chunk %d/%d worker returned invalid JSON: %s',
                                $item['chunkNum'],
                                $totalChunks,
                                substr($item['process']->getOutput(), 0, 200),
                            ), 0, $e);
                            continue;
                        }
                        foreach ($result['logs'] as $msg) {
                            $this->logger->info($msg);
                        }
                        $writeQueue[] = ['fileId' => $result['fileId'], 'chunkNum' => $item['chunkNum']];
                    }
                }

                // --- Phase 1b: start new workers into freed slots ---
                while ($chunkIndex < $totalChunks && count($runningProcesses) < max(1, $this->maxParallelism)) {
                    $chunkNum = $chunkIndex + 1;
                    $this->logger->info(sprintf(
                        'Starting chunk %d/%d (%d slices)',
                        $chunkNum,
                        $totalChunks,
                        count($chunks[$chunkIndex]),
                    ));
                    $process = $processFactory([
                        'sourceApiUrl' => $sourceApiUrl,
                        'sourceToken' => $sourceToken,
                        'targetApiUrl' => $targetApiUrl,
                        'targetToken' => $targetToken,
                        'fileId' => $fileId,
                        'bucket' => $bucket,
                        'chunk' => $chunks[$chunkIndex],
                        'optionFileName' => $tableInfo['id'],
                        'chunkNum' => $chunkNum,
                        'totalChunks' => $totalChunks,
                    ]);
                    $process->start();
                    $runningProcesses[$chunkIndex] = ['process' => $process, 'chunkNum' => $chunkNum];
                    $chunkIndex++;
                    $didSomething = true;
                }

                // --- Phase 2a: reap finished SAPI import jobs ---
                foreach ($inFlightImports as $key => $item) {
                    try {
                        $job = $this->targetClient->getJob($item['jobId']);
                    } catch (Throwable $e) {
                        unset($inFlightImports[$key]);
                        $errors['import-' . $key] = new RuntimeException(sprintf(
                            'Chunk %d/%d: polling import job %d failed: %s',
                            $item['chunkNum'],
                            $totalChunks,
                            $item['jobId'],
                            $e->getMessage(),
                        ), 0, $e);
                        $didSomething = true;
                        continue;
                    }
                    $status = (string) $job['status'];
                    if (!in_array($status, ['success', 'error'], true)) {
                        continue; // still waiting/processing
                    }
                    unset($inFlightImports[$key]);
                    $didSomething = true;
                    if ($status === 'success') {
                        $this->logger->info(sprintf(
                            'Finished chunk %d/%d (import job %d)',
                            $item['chunkNum'],
                            $totalChunks,
                            $item['jobId'],
                        ));
                    } else {
                        $errors['import-' . $key] = new RuntimeException(sprintf(
                            'Chunk %d/%d import job %d failed: %s (fileId: %s)',
                            $item['chunkNum'],
                            $totalChunks,
                            $item['jobId'],
                            $job['error']['message'] ?? '(no error message)',
                            $item['fileId'],
                        ));
                    }
                }

                // --- Phase 2b: enqueue new SAPI imports up to parallelImports ---
                while (!empty($writeQueue) && count($inFlightImports) < max(1, $this->parallelImports)) {
                    $writeItem = array_shift($writeQueue);
                    $this->logger->info(sprintf(
                        'Chunk %d/%d: starting import into table %s (fileId: %s)',
                        $writeItem['chunkNum'],
                        $totalChunks,
                        $tableInfo['id'],
                        $writeItem['fileId'],
                    ));
                    $jobId = $this->targetClient->queueTableImport($tableInfo['id'], [
                        'name' => $tableInfo['name'],
                        'dataFileId' => $writeItem['fileId'],
                        'columns' => $tableInfo['columns'],
                        'useTimestampFromDataFile' => $preserveTimestamp,
                        'incremental' => true,
                    ]);
                    $inFlightImports[$writeItem['chunkNum']] = [
                        'jobId' => (int) $jobId,
                        'chunkNum' => $writeItem['chunkNum'],
                        'fileId' => $writeItem['fileId'],
                    ];
                    $didSomething = true;
                }

                if (!$didSomething) {
                    usleep(1_000_000); // 1s — nothing to process, back off polling
                }
            }

            $this->logger->info(sprintf('All %d chunks processed', $totalChunks - $startChunkIndex));
        } finally {
            foreach ($runningProcesses as $item) {
                $item['process']->stop(0);
            }

            if (!empty($primaryKey)) {
                $this->logger->info(sprintf(
                    'Restoring primary key [%s] on %s',
                    implode(', ', $primaryKey),
                    $tableInfo['id'],
                ));
                $this->targetClient->createTablePrimaryKey($tableInfo['id'], $primaryKey);
            }
        }

        if (!empty($errors)) {
            $first = reset($errors);
            throw new RuntimeException(
                sprintf('Failed %d chunk(s). First: %s', count($errors), $first->getMessage()),
                0,
                $first,
            );
        }
    }

    private function getGcsClient(int $fileId): GoogleStorageClient
    {
        $fileInfo = $this->sourceClient->getFile(
            $fileId,
            (new GetFileOptions())->setFederationToken(true),
        );
        $gcsCredentials = $fileInfo['gcsCredentials'];

        $fetchAuthToken = new class ([
            'access_token' => $gcsCredentials['access_token'],
            'expires_in' => $gcsCredentials['expires_in'],
            'token_type' => $gcsCredentials['token_type'],
        ]) implements FetchAuthTokenInterface {
            public function __construct(private array $creds)
            {
            }

            public function fetchAuthToken(?callable $httpHandler = null): array
            {
                return $this->creds;
            }

            public function getCacheKey(): string
            {
                return '';
            }

            public function getLastReceivedToken(): array
            {
                return $this->creds;
            }
        };

        return new GoogleStorageClient([
            'projectId' => $gcsCredentials['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);
    }
}
