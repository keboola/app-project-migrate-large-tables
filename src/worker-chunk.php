<?php

declare(strict_types=1);

use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Symfony\Component\Filesystem\Filesystem;

require __DIR__ . '/../vendor/autoload.php';

/** @var array{
 *   sourceApiUrl: string,
 *   sourceToken: string,
 *   targetApiUrl: string,
 *   targetToken: string,
 *   fileId: int,
 *   bucket: string,
 *   chunk: array<array{url: string}>,
 *   optionFileName: string,
 *   chunkNum: int,
 *   totalChunks: int,
 * } $input
 */
$input = json_decode((string) stream_get_contents(STDIN), true);

$sourceClient = new Client(['url' => $input['sourceApiUrl'], 'token' => $input['sourceToken']]);
$targetClient = new Client(['url' => $input['targetApiUrl'], 'token' => $input['targetToken']]);

$logs = [];
$chunkNum = $input['chunkNum'];
$totalChunks = $input['totalChunks'];
$chunk = $input['chunk'];
$bucket = $input['bucket'];
$fileId = $input['fileId'];

try {
    // Refresh GCS credentials for this chunk
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

    $tmpDir = sys_get_temp_dir() . '/chunk-' . uniqid();
    mkdir($tmpDir, 0777, true);

    $slices = [];
    $logs[] = sprintf(
        'Chunk %d/%d: downloading %d slices from GCS',
        $chunkNum,
        $totalChunks,
        count($chunk),
    );

    /** @var array{"url": string} $entry */
    foreach ($chunk as $entry) {
        $slices[] = $destinationFile = $tmpDir . '/' . basename($entry['url']);
        $blobPath = explode(sprintf('/%s/', $bucket), $entry['url']);
        $retBucket->object($blobPath[1])->downloadToFile($destinationFile);
    }

    $logs[] = sprintf('Chunk %d/%d: uploading to SAPI file storage', $chunkNum, $totalChunks);

    $optionUploadedFile = new FileUploadOptions();
    $optionUploadedFile
        ->setFederationToken(true)
        ->setFileName($input['optionFileName'])
        ->setIsSliced(true)
    ;

    $destinationFileId = $targetClient->uploadSlicedFile($slices, $optionUploadedFile);
    $logs[] = sprintf(
        'Chunk %d/%d: uploaded (fileId: %d)',
        $chunkNum,
        $totalChunks,
        $destinationFileId,
    );

    $fs = new Filesystem();
    $fs->remove($slices);
    $fs->remove($tmpDir);

    echo json_encode(['fileId' => (string) $destinationFileId, 'logs' => $logs]);
    exit(0);
} catch (Throwable $e) {
    fwrite(STDERR, sprintf(
        'Chunk %d/%d failed: %s in %s:%d',
        $chunkNum,
        $totalChunks,
        $e->getMessage(),
        $e->getFile(),
        $e->getLine(),
    ));
    exit(1);
}
