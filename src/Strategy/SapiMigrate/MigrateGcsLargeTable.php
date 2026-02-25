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
use Symfony\Component\Filesystem\Filesystem;

class MigrateGcsLargeTable
{
    private const CHUNK_SIZE = 500;

    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
    ) {
    }

    public function migrate(
        int $fileId,
        array $tableInfo,
        bool $preserveTimestamp,
        Temp $tmpFolder,
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

        foreach ($chunks as $chunkKey => $chunk) {
            $this->logger->info(sprintf('Processing %s/%s chunk', $chunkKey+1, count($chunks)));
            $slices = [];
            // refresh credentials for each chunk
            $gcsClient = $this->getGcsClient($fileId);
            $retBucket = $gcsClient->bucket($bucket);
            /** @var array{"url": string} $entry */
            foreach ($chunk as $entry) {
                $slices[] = $destinationFile = $tmpFolder->getTmpFolder() . '/' . basename($entry['url']);

                $sprintf = sprintf(
                    '/%s/',
                    $fileInfo['gcsPath']['bucket'],
                );
                $blobPath = explode($sprintf, $entry['url']);
                $retBucket->object($blobPath[1])->downloadToFile($destinationFile);
            }

            $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);

            $this->targetClient->writeTableAsyncDirect(
                $tableInfo['id'],
                [
                    'name' => $tableInfo['name'],
                    'dataFileId' => $destinationFileId,
                    'columns' => $tableInfo['columns'],
                    'useTimestampFromDataFile' => $preserveTimestamp,
                    'incremental' => true,
                ],
            );

            $fs = new Filesystem();
            foreach ($slices as $slice) {
                $fs->remove($slice);
            }
        }
    }

    private function getGcsClient(int $fileId): GoogleStorageClient
    {
        $fileInfo = $this->sourceClient->getFile(
            $fileId,
            (new GetFileOptions())->setFederationToken(true),
        );
        $gcsCredentials = $fileInfo['gcsCredentials'];
        $options = [
            'credentials' => [
                'access_token' => $gcsCredentials['access_token'],
                'expires_in' => $gcsCredentials['expires_in'],
                'token_type' => $gcsCredentials['token_type'],
            ],
            'projectId' => $gcsCredentials['projectId'],
        ];

        $fetchAuthToken = $this->getAuthTokenClass($options['credentials']);
        return new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);
    }

    private function getAuthTokenClass(array $credentials): FetchAuthTokenInterface
    {
        return new class ($credentials) implements FetchAuthTokenInterface {

            public function __construct(
                private array $creds,
            ) {
            }

            public function fetchAuthToken(?callable $httpHandler = null)
            {
                return $this->creds;
            }

            public function getCacheKey()
            {
                return '';
            }

            public function getLastReceivedToken()
            {
                return $this->creds;
            }
        };
    }
}
