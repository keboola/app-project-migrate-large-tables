<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate\MigrateGcsLargeTable;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;

class SapiMigrate implements MigrateInterface
{
    private const LARGE_GCS_TABLE_SIZE = 50*1000*1000*1000; // 50 GB
    private StorageModifier $storageModifier;
    private MigrateGcsLargeTable $migrateGcsLargeTable;

    /** @var string[] $bucketsExist */
    private array $bucketsExist = [];

    /** @var array<string, string> $destinationBackendCache */
    private array $destinationBackendCache = [];

    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
        private readonly int $parallelChunks = 3,
        private readonly int $chunkSize = 150,
    ) {
        $this->storageModifier = new StorageModifier($this->targetClient);
        $this->migrateGcsLargeTable = new MigrateGcsLargeTable(
            $this->sourceClient,
            $this->targetClient,
            $this->logger,
            $this->dryRun,
            $this->parallelChunks,
            $this->chunkSize,
        );
    }

    public function migrate(Config $config): void
    {
        $tableIds = $config->getMigrateTables() ?: $this->getAllTables($config->isIncremental());
        foreach ($tableIds as $tableId) {
            try {
                $tableInfo = $this->sourceClient->getTable($tableId);
            } catch (ClientException $e) {
                $this->logger->warning(sprintf(
                    'Skipping migration Table ID "%s". Reason: "%s".',
                    $tableId,
                    $e->getMessage(),
                ));
                continue;
            }
            if ($tableInfo['bucket']['stage'] === 'sys') {
                $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $tableInfo['id']));
                continue;
            }

            if ($tableInfo['isAlias']) {
                $this->logger->warning(sprintf('Skipping table %s (alias)', $tableInfo['id']));
                continue;
            }

            if (!in_array($tableInfo['bucket']['id'], $this->bucketsExist) &&
                !$this->targetClient->bucketExists($tableInfo['bucket']['id'])) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating bucket %s', $tableInfo['bucket']['id']));
                } else {
                    $this->logger->info(sprintf('Creating bucket %s', $tableInfo['bucket']['id']));
                    $this->bucketsExist[] = $tableInfo['bucket']['id'];

                    $this->storageModifier->createBucket($tableInfo['bucket']['id']);
                }
            }

            if (!$this->targetClient->tableExists($tableId)) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating table %s', $tableInfo['id']));
                } else {
                    $this->logger->info(sprintf('Creating table %s', $tableInfo['id']));
                    $this->storageModifier->createTable($tableInfo, $config->forcePrimaryKeyNotNull());
                }
            }

            $this->migrateTable($tableInfo, $config);
        }
    }

    private function migrateTable(array $sourceTableInfo, Config $config): void
    {
        if ($this->dryRun) {
            $this->logger->info(sprintf('[dry-run] Migrate table %s', $sourceTableInfo['id']));
            return;
        }

        $changedSince = $this->resolveChangedSince($sourceTableInfo, $config);
        if ($changedSince !== null) {
            $this->logger->info(sprintf(
                'Incremental export of table %s (changedSince: %s)',
                $sourceTableInfo['id'],
                $changedSince,
            ));
        } else {
            $this->logger->info(sprintf('Exporting table %s', $sourceTableInfo['id']));
        }

        $file = $this->sourceClient->exportTableAsync(
            $sourceTableInfo['id'],
            $this->buildExportOptions($sourceTableInfo, $config, $changedSince),
        );

        $sourceFileId = $file['file']['id'];
        $sourceFileInfo = $this->sourceClient->getFile($sourceFileId);

        if ($sourceFileInfo['provider'] === 'gcp' &&
            $sourceFileInfo['isSliced'] === true &&
            $sourceFileInfo['sizeBytes'] > self::LARGE_GCS_TABLE_SIZE
        ) {
            $this->migrateGcsLargeTable->migrate(
                $sourceFileId,
                $sourceTableInfo,
                $config->preserveTimestamp(),
                null,
                null,
                $config->isIncremental(),
            );
            return;
        }

        $tmp = new Temp();
        $optionUploadedFile = new FileUploadOptions();
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($sourceTableInfo['id'])
        ;
        if ($sourceFileInfo['isSliced'] === true) {
            $optionUploadedFile->setIsSliced(true);

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $slices = $this->sourceClient->downloadSlicedFile($sourceFileId, $tmp->getTmpFolder());

            $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
            $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);
        } else {
            $fileName = $tmp->getTmpFolder() . '/' . $sourceFileInfo['name'];

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $this->sourceClient->downloadFile($sourceFileId, $fileName);

            $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
            $destinationFileId = $this->targetClient->uploadFile($fileName, $optionUploadedFile);
        }

        // Upload data to table
        $writeOptions = [
            'name' => $sourceTableInfo['name'],
            'dataFileId' => $destinationFileId,
            'columns' => $sourceTableInfo['columns'],
            'useTimestampFromDataFile' => $config->preserveTimestamp(),
        ];
        if ($config->isIncremental()) {
            $writeOptions['incremental'] = true;
        }
        $this->targetClient->writeTableAsyncDirect(
            $sourceTableInfo['id'],
            $writeOptions,
        );

        $tmp->remove();
    }

    /**
     * For incremental migration, resolve the changedSince parameter
     * based on the max _timestamp value in the target table.
     */
    private function resolveChangedSince(array $sourceTableInfo, Config $config): ?string
    {
        if (!$config->isIncremental()) {
            return null;
        }

        if (!$this->targetClient->tableExists($sourceTableInfo['id'])) {
            return null;
        }

        $targetTableInfo = $this->targetClient->getTable($sourceTableInfo['id']);
        if (($targetTableInfo['rowsCount'] ?? 0) === 0) {
            return null;
        }

        return $this->getMaxTimestamp($sourceTableInfo['id']);
    }

    /**
     * Get the max _timestamp value from a target table by exporting
     * a single row ordered by _timestamp descending.
     *
     * Uses includeInternalTimestamp instead of columns filter because
     * _timestamp is a system column not available in typed table column definitions.
     */
    private function getMaxTimestamp(string $tableId): ?string
    {
        $file = $this->targetClient->exportTableAsync($tableId, [
            'orderBy' => [
                [
                    'column' => '_timestamp',
                    'order' => 'DESC',
                ],
            ],
            'limit' => 1,
            'includeInternalTimestamp' => true,
        ]);

        $sourceFileId = $file['file']['id'];
        $tmp = new Temp();
        $fileName = $tmp->getTmpFolder() . '/max_timestamp.csv';
        $this->targetClient->downloadFile($sourceFileId, $fileName);

        $content = file_get_contents($fileName);
        $tmp->remove();

        if ($content === false) {
            return null;
        }

        $lines = array_filter(explode("\n", trim($content)));
        // First line is header, second line is data — _timestamp is the last column
        if (count($lines) < 2) {
            return null;
        }

        $header = str_getcsv($lines[1]);
        $headerColumns = str_getcsv($lines[0]);
        $timestampIndex = array_search('"_timestamp"', $headerColumns);
        if ($timestampIndex === false) {
            $timestampIndex = array_search('_timestamp', $headerColumns);
        }
        if ($timestampIndex === false) {
            return null;
        }

        $maxTimestamp = $header[$timestampIndex] ?? null;
        if ($maxTimestamp === null || $maxTimestamp === '') {
            return null;
        }

        return trim($maxTimestamp, '"');
    }

    private function getAllTables(bool $incremental = false): array
    {
        $buckets = $this->sourceClient->listBuckets();
        $listTables = [];
        foreach ($buckets as $bucket) {
            $sourceBucketTables = $this->sourceClient->listTables($bucket['id']);

            if ($incremental) {
                // In incremental mode, include all source tables (even if target already has rows)
                array_unshift(
                    $listTables,
                    ...array_map(fn($v) => $v['id'], $sourceBucketTables),
                );
                continue;
            }

            if (!$this->targetClient->bucketExists($bucket['id'])) {
                $targetBucketTables = [];
            } else {
                $targetBucketTables = $this->targetClient->listTables($bucket['id']);
            }

            $filteredBucketTables = array_filter(
                $sourceBucketTables,
                function ($sourceTable) use ($targetBucketTables) {
                    $v = current(array_filter(
                        $targetBucketTables,
                        fn($v) => $v['id'] === $sourceTable['id'],
                    ));
                    return empty($v) || $v['rowsCount'] === 0 || is_null($v['rowsCount']);
                },
            );

            array_unshift(
                $listTables,
                ...array_map(fn($v) => $v['id'], $filteredBucketTables),
            );
        }
        return $listTables;
    }

    /** @param array<string, mixed> $sourceTableInfo */
    private function buildExportOptions(array $sourceTableInfo, Config $config, ?string $changedSince = null): array
    {
        $options = [
            'gzip' => true,
            'includeInternalTimestamp' => $config->preserveTimestamp(),
        ];

        if ($changedSince !== null) {
            $options['changedSince'] = $changedSince;
        }

        $sourceBucket = $sourceTableInfo['bucket'];
        if (!is_array($sourceBucket)) {
            return $options;
        }
        $sourceBackend = (string) ($sourceBucket['backend'] ?? '');
        $sourceBucketId = (string) ($sourceBucket['id'] ?? '');
        if ($sourceBackend === '' || $sourceBucketId === '') {
            return $options;
        }
        if ($sourceBackend === 'snowflake'
            && $this->getDestinationBucketBackend($sourceBucketId) === 'bigquery'
        ) {
            $options['timezone'] = 'UTC';
        }

        return $options;
    }

    private function getDestinationBucketBackend(string $bucketId): string
    {
        if (!array_key_exists($bucketId, $this->destinationBackendCache)) {
            $bucket = $this->targetClient->getBucket($bucketId);
            $this->destinationBackendCache[$bucketId] = (string) ($bucket['backend'] ?? '');
        }

        return $this->destinationBackendCache[$bucketId];
    }
}
