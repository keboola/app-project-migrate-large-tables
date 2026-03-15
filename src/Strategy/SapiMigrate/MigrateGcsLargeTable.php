<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use GuzzleHttp\Utils;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Symfony\Component\Process\Process;

class MigrateGcsLargeTable
{
    private const CHUNK_SIZE = 50;

    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
        private readonly int $maxParallelism = 4,
    ) {
    }

    public function migrate(
        int $fileId,
        array $tableInfo,
        bool $preserveTimestamp,
    ): void {
        if ($this->dryRun === true) {
            $this->logger->info(sprintf('[dry-run] Migrate table %s', $tableInfo['id']));
            return;
        }

        $fileInfo = $this->sourceClient->getFile(
            $fileId,
            (new GetFileOptions())->setFederationToken(true),
        );

        $bucket = $fileInfo['gcsPath']['bucket'];
        $gcsClient = $this->getGcsClient($fileId);
        $retBucket = $gcsClient->bucket($bucket);
        $manifestObject = $retBucket->object($fileInfo['gcsPath']['key'] . 'manifest')->downloadAsString();

        /** @var array{"entries": string[]} $manifest */
        $manifest = Utils::jsonDecode($manifestObject, true);
        $chunks = array_chunk((array) $manifest['entries'], self::CHUNK_SIZE);

        $totalChunks = count($chunks);
        $this->logger->info(sprintf(
            'Processing table %s: %d chunks with parallelism %d',
            $tableInfo['id'],
            $totalChunks,
            $this->maxParallelism,
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

        $chunkWorker = __DIR__ . '/../../worker-chunk.php';

        /** @var array<int, array{process: Process, chunkNum: int}> $runningProcesses */
        $runningProcesses = [];
        /** @var array<array{fileId: string, chunkNum: int}> $writeQueue */
        $writeQueue = [];
        $errors = [];
        $chunkIndex = 0;

        try {
            while ($chunkIndex < $totalChunks || !empty($runningProcesses) || !empty($writeQueue)) {
                // --- Phase 1: spustit nové procesy ---
                while ($chunkIndex < $totalChunks && count($runningProcesses) < $this->maxParallelism) {
                    $chunkNum = $chunkIndex + 1;
                    $this->logger->info(sprintf(
                        'Starting chunk %d/%d (%d slices)',
                        $chunkNum,
                        $totalChunks,
                        count($chunks[$chunkIndex]),
                    ));
                    $process = new Process(
                        [PHP_BINARY, $chunkWorker],
                        null,
                        null,
                        json_encode([
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
                        ]),
                        null,
                    );
                    $process->start();
                    $runningProcesses[$chunkIndex] = ['process' => $process, 'chunkNum' => $chunkNum];
                    $chunkIndex++;
                }

                // --- Phase 1: zkontrolovat dokončené ---
                foreach ($runningProcesses as $key => $item) {
                    if (!$item['process']->isRunning()) {
                        unset($runningProcesses[$key]);
                        if (!$item['process']->isSuccessful()) {
                            $errors[$key] = new RuntimeException(sprintf(
                                'Chunk %d/%d worker exited with code %d: %s',
                                $item['chunkNum'],
                                $totalChunks,
                                $item['process']->getExitCode(),
                                trim($item['process']->getErrorOutput()),
                            ));
                            continue;
                        }
                        /** @var array{logs: string[], fileId: string} $result */
                        $result = json_decode($item['process']->getOutput(), true);
                        foreach ($result['logs'] as $msg) {
                            $this->logger->info($msg);
                        }
                        $writeQueue[] = ['fileId' => $result['fileId'], 'chunkNum' => $item['chunkNum']];
                    }
                }

                // --- Phase 2: zpracovat první položku z writeQueue (blokující) ---
                // Běžící Phase 1 procesy pokračují v OS i během tohoto blokujícího volání.
                if (!empty($writeQueue)) {
                    $writeItem = array_shift($writeQueue);
                    $this->logger->info(sprintf(
                        'Chunk %d/%d: importing into table %s (fileId: %s)',
                        $writeItem['chunkNum'],
                        $totalChunks,
                        $tableInfo['id'],
                        $writeItem['fileId'],
                    ));
                    $this->targetClient->writeTableAsyncDirect($tableInfo['id'], [
                        'name' => $tableInfo['name'],
                        'dataFileId' => $writeItem['fileId'],
                        'columns' => $tableInfo['columns'],
                        'useTimestampFromDataFile' => $preserveTimestamp,
                        'incremental' => true,
                    ]);
                    $this->logger->info(sprintf('Finished chunk %d/%d', $writeItem['chunkNum'], $totalChunks));
                    continue;
                }

                usleep(100_000); // 100ms polling pokud není co zpracovat
            }

            $this->logger->info(sprintf('All %d chunks processed', $totalChunks));
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
