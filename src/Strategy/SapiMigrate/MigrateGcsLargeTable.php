<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate;

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use GuzzleHttp\Utils;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Spatie\Async\Pool;
use Symfony\Component\Filesystem\Filesystem;
use Throwable;

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

        $optionUploadedFile = new FileUploadOptions();
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($tableInfo['id'])
            ->setIsSliced(true)
        ;

        $totalChunks = count($chunks);
        $this->logger->info(sprintf(
            'Processing table %s: %d chunks with parallelism %d',
            $tableInfo['id'],
            $totalChunks,
            $this->maxParallelism,
        ));

        // Extract primitives — closures passed to spatie/async are serialized,
        // so we must not capture $this (which holds non-serializable CurlHandles).
        $sourceApiUrl = $this->sourceClient->getApiUrl();
        $sourceToken = $this->sourceClient->getTokenString();
        $targetApiUrl = $this->targetClient->getApiUrl();
        $targetToken = $this->targetClient->getTokenString();

        $pool = Pool::create()->concurrency($this->maxParallelism);
        $errors = [];

        foreach ($chunks as $chunkKey => $chunk) {
            $chunkNum = $chunkKey + 1;
            $this->logger->info(sprintf('Queuing chunk %d/%d (%d slices)', $chunkNum, $totalChunks, count($chunk)));

            // Child process: download from GCS + upload to SAPI + trigger import.
            // Running the import inside the child keeps ->then() lightweight so the
            // pool loop is never blocked and other children can run concurrently.
            $pool
                ->add(static function () use (
                    $chunk,
                    $fileId,
                    $bucket,
                    $fileInfo,
                    $optionUploadedFile,
                    $sourceApiUrl,
                    $sourceToken,
                    $targetApiUrl,
                    $targetToken,
                    $tableInfo,
                    $preserveTimestamp,
                    $chunkNum,
                    $totalChunks,
                ): array {
                    $logs = [];
                    $sourceClient = new Client(['url' => $sourceApiUrl, 'token' => $sourceToken]);
                    $targetClient = new Client(['url' => $targetApiUrl, 'token' => $targetToken]);

                    // Refresh GCS credentials for each chunk
                    $logs[] = sprintf('Chunk %d/%d: refreshing GCS credentials', $chunkNum, $totalChunks);
                    $chunkFileInfo = $sourceClient->getFile(
                        $fileId,
                        (new GetFileOptions())->setFederationToken(true),
                    );
                    $gcsCredentials = $chunkFileInfo['gcsCredentials'];

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

                    $gcsClient = new GoogleStorageClient([
                        'projectId' => $gcsCredentials['projectId'],
                        'credentialsFetcher' => $fetchAuthToken,
                    ]);
                    $retBucket = $gcsClient->bucket($bucket);

                    $chunkTmpFolder = new Temp();
                    $slices = [];

                    $logs[] = sprintf(
                        'Chunk %d/%d: downloading %d slices from GCS',
                        $chunkNum,
                        $totalChunks,
                        count($chunk),
                    );
                    /** @var array{"url": string} $entry */
                    foreach ($chunk as $entry) {
                        $slices[] = $destinationFile = $chunkTmpFolder->getTmpFolder() . '/' . basename($entry['url']);
                        $blobPath = explode(sprintf('/%s/', $fileInfo['gcsPath']['bucket']), $entry['url']);
                        $retBucket->object($blobPath[1])->downloadToFile($destinationFile);
                    }

                    $logs[] = sprintf('Chunk %d/%d: uploading to SAPI file storage', $chunkNum, $totalChunks);
                    $destinationFileId = $targetClient->uploadSlicedFile($slices, $optionUploadedFile);
                    $logs[] = sprintf(
                        'Chunk %d/%d: uploaded (fileId: %d)',
                        $chunkNum,
                        $totalChunks,
                        $destinationFileId,
                    );

                    $logs[] = sprintf(
                        'Chunk %d/%d: importing into table %s',
                        $chunkNum,
                        $totalChunks,
                        $tableInfo['id'],
                    );
                    $targetClient->writeTableAsyncDirect(
                        $tableInfo['id'],
                        [
                            'name' => $tableInfo['name'],
                            'dataFileId' => $destinationFileId,
                            'columns' => $tableInfo['columns'],
                            'useTimestampFromDataFile' => $preserveTimestamp,
                            'incremental' => true,
                        ],
                    );
                    $logs[] = sprintf('Chunk %d/%d: import done', $chunkNum, $totalChunks);

                    $fs = new Filesystem();
                    foreach ($slices as $slice) {
                        $fs->remove($slice);
                    }
                    $chunkTmpFolder->remove();

                    return $logs;
                })
                ->then(function (array $logs) use ($chunkNum, $totalChunks): void {
                    foreach ($logs as $message) {
                        $this->logger->info($message);
                    }
                    $this->logger->info(sprintf('Finished chunk %d/%d', $chunkNum, $totalChunks));
                })
                ->catch(function (Throwable $e) use ($chunkKey, $chunkNum, $totalChunks, &$errors): void {
                    $this->logger->error(sprintf(
                        'Failed chunk %d/%d: %s',
                        $chunkNum,
                        $totalChunks,
                        $e->getMessage(),
                    ));
                    $errors[$chunkKey] = $e;
                });
        }

        $pool->wait();
        $this->logger->info(sprintf('All %d chunks processed', $totalChunks));

        if (!empty($errors)) {
            $firstError = reset($errors);
            throw new RuntimeException(
                sprintf(
                    'Failed to process %d chunk(s). First error (chunk %d): %s',
                    count($errors),
                    array_key_first($errors) + 1,
                    $firstError->getMessage(),
                ),
                0,
                $firstError,
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
