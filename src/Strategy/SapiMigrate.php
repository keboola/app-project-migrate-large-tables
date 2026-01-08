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

    public function __construct(
        private readonly Client $sourceClient,
        private readonly Client $targetClient,
        private readonly LoggerInterface $logger,
        private readonly bool $dryRun = false,
    ) {
        $this->storageModifier = new StorageModifier($this->targetClient);
        $this->migrateGcsLargeTable = new MigrateGcsLargeTable(
            $this->sourceClient,
            $this->targetClient,
            $this->logger,
            $this->dryRun,
        );
    }

    public function migrate(Config $config): void
    {
        $tableIds = $config->getMigrateTables() ?: $this->getAllTables();
        $concurrency = $config->getConcurrency();

        if ($concurrency > 1) {
            $this->migrateTablesInParallel($tableIds, $config, $concurrency);
        } else {
            $this->migrateTablesSequentially($tableIds, $config);
        }
    }

    /**
     * @param string[] $tableIds
     */
    private function migrateTablesSequentially(array $tableIds, Config $config): void
    {
        foreach ($tableIds as $tableId) {
            $this->processTable($tableId, $config);
        }
    }

    /**
     * @param string[] $tableIds
     */
    private function migrateTablesInParallel(array $tableIds, Config $config, int $concurrency): void
    {
        $batches = array_chunk($tableIds, max(1, $concurrency));
        $totalBatches = count($batches);

        foreach ($batches as $batchIndex => $batch) {
            $this->logger->info(sprintf(
                'Processing batch %d/%d (%d tables)',
                $batchIndex + 1,
                $totalBatches,
                count($batch),
            ));

            $tableInfos = [];
            foreach ($batch as $tableId) {
                try {
                    $tableInfo = $this->sourceClient->getTable($tableId);
                    if ($tableInfo['bucket']['stage'] === 'sys') {
                        $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $tableInfo['id']));
                        continue;
                    }
                    if ($tableInfo['isAlias']) {
                        $this->logger->warning(sprintf('Skipping table %s (alias)', $tableInfo['id']));
                        continue;
                    }
                    $tableInfos[] = $tableInfo;
                } catch (ClientException $e) {
                    $this->logger->warning(sprintf(
                        'Skipping migration Table ID "%s". Reason: "%s".',
                        $tableId,
                        $e->getMessage(),
                    ));
                }
            }

            $this->ensureBucketsExist($tableInfos);
            $this->ensureTablesExist($tableInfos);

            if ($this->dryRun && $config->shouldSkipExportInDryRun()) {
                foreach ($tableInfos as $tableInfo) {
                    $this->logger->info(sprintf(
                        '[dry-run] Would migrate table %s (rows: %s, size: %s bytes)',
                        $tableInfo['id'],
                        $tableInfo['rowsCount'] ?? 'unknown',
                        $tableInfo['dataSizeBytes'] ?? 'unknown',
                    ));
                }
                continue;
            }

            $exportJobs = $this->startExportJobs($tableInfos, $config);

            foreach ($exportJobs as $index => $exportJob) {
                $tableInfo = $tableInfos[$index];
                $this->processExportedTable($tableInfo, $exportJob, $config);
            }
        }
    }

    /**
     * @param array<array<string, mixed>> $tableInfos
     */
    private function ensureBucketsExist(array $tableInfos): void
    {
        foreach ($tableInfos as $tableInfo) {
            $bucket = $tableInfo['bucket'];
            assert(is_array($bucket) && isset($bucket['id']) && is_string($bucket['id']));
            $bucketId = $bucket['id'];
            if (!in_array($bucketId, $this->bucketsExist, true) &&
                !$this->targetClient->bucketExists($bucketId)) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating bucket %s', $bucketId));
                } else {
                    $this->logger->info(sprintf('Creating bucket %s', $bucketId));
                    $this->storageModifier->createBucket($bucketId);
                }
                $this->bucketsExist[] = $bucketId;
            }
        }
    }

    /**
     * @param array<array<string, mixed>> $tableInfos
     */
    private function ensureTablesExist(array $tableInfos): void
    {
        foreach ($tableInfos as $tableInfo) {
            assert(isset($tableInfo['id']) && is_string($tableInfo['id']));
            $tableId = $tableInfo['id'];
            if (!$this->targetClient->tableExists($tableId)) {
                if ($this->dryRun) {
                    $this->logger->info(sprintf('[dry-run] Creating table %s', $tableId));
                } else {
                    $this->logger->info(sprintf('Creating table %s', $tableId));
                    $this->storageModifier->createTable($tableInfo);
                }
            }
        }
    }

    /**
     * @param array<array<string, mixed>> $tableInfos
     * @return array<array<string, mixed>>
     */
    private function startExportJobs(array $tableInfos, Config $config): array
    {
        $exportJobs = [];
        foreach ($tableInfos as $tableInfo) {
            assert(isset($tableInfo['id']) && is_string($tableInfo['id']));
            $tableId = $tableInfo['id'];
            $this->logger->info(sprintf('Exporting table %s', $tableId));
            $file = $this->sourceClient->exportTableAsync($tableId, [
                'gzip' => true,
                'includeInternalTimestamp' => $config->preserveTimestamp(),
            ]);
            $exportJobs[] = $file;
        }
        return $exportJobs;
    }

    /**
     * @param array<string, mixed> $sourceTableInfo
     * @param array<string, mixed> $exportJob
     */
    private function processExportedTable(array $sourceTableInfo, array $exportJob, Config $config): void
    {
        $file = $exportJob['file'];
        assert(is_array($file) && isset($file['id']) && is_numeric($file['id']));
        $sourceFileId = (int) $file['id'];
        $sourceFileInfo = $this->sourceClient->getFile($sourceFileId);
        assert(isset($sourceTableInfo['id']) && is_string($sourceTableInfo['id']));
        $tableId = $sourceTableInfo['id'];

        $tmp = new Temp();
        $optionUploadedFile = new FileUploadOptions();
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($tableId)
        ;
        $tableSize = $sourceFileInfo['sizeBytes'];
        if ($sourceFileInfo['provider'] === 'gcp' &&
            $sourceFileInfo['isSliced'] === true &&
            $tableSize > self::LARGE_GCS_TABLE_SIZE
        ) {
            $this->migrateGcsLargeTable->migrate(
                $sourceFileId,
                $sourceTableInfo,
                $config->preserveTimestamp(),
                $tmp,
            );
            $tmp->remove();
            return;
        } elseif ($sourceFileInfo['isSliced'] === true) {
            $optionUploadedFile->setIsSliced(true);

            if ($this->dryRun === false) {
                $this->logger->info(sprintf('Downloading table %s', $tableId));
                $slices = $this->sourceClient->downloadSlicedFile($sourceFileId, $tmp->getTmpFolder());

                $this->logger->info(sprintf('Uploading table %s', $tableId));
                $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);
            } else {
                $this->logger->info(sprintf('[dry-run] Migrate table %s', $tableId));
                $destinationFileId = null;
            }
        } else {
            $fileName = $tmp->getTmpFolder() . '/' . $sourceFileInfo['name'];

            if ($this->dryRun === false) {
                $this->logger->info(sprintf('Downloading table %s', $tableId));
                $this->sourceClient->downloadFile($sourceFileId, $fileName);

                $this->logger->info(sprintf('Uploading table %s', $tableId));
                $destinationFileId = $this->targetClient->uploadFile($fileName, $optionUploadedFile);
            } else {
                $this->logger->info(sprintf('[dry-run] Uploading table %s', $tableId));
                $destinationFileId = null;
            }
        }

        if ($this->dryRun === false) {
            $this->targetClient->writeTableAsyncDirect(
                $tableId,
                [
                    'name' => $sourceTableInfo['name'],
                    'dataFileId' => $destinationFileId,
                    'columns' => $sourceTableInfo['columns'],
                    'useTimestampFromDataFile' => $config->preserveTimestamp(),
                ],
            );
        } else {
            assert(isset($sourceTableInfo['name']) && is_string($sourceTableInfo['name']));
            $this->logger->info(sprintf('[dry-run] Import data to table "%s"', $sourceTableInfo['name']));
        }

        $tmp->remove();
    }

    private function processTable(string $tableId, Config $config): void
    {
        try {
            $tableInfo = $this->sourceClient->getTable($tableId);
        } catch (ClientException $e) {
            $this->logger->warning(sprintf(
                'Skipping migration Table ID "%s". Reason: "%s".',
                $tableId,
                $e->getMessage(),
            ));
            return;
        }
        if ($tableInfo['bucket']['stage'] === 'sys') {
            $this->logger->warning(sprintf('Skipping table %s (sys bucket)', $tableInfo['id']));
            return;
        }

        if ($tableInfo['isAlias']) {
            $this->logger->warning(sprintf('Skipping table %s (alias)', $tableInfo['id']));
            return;
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
                $this->storageModifier->createTable($tableInfo);
            }
        }

        if ($this->dryRun && $config->shouldSkipExportInDryRun()) {
            $this->logger->info(sprintf(
                '[dry-run] Would migrate table %s (rows: %s, size: %s bytes)',
                $tableInfo['id'],
                $tableInfo['rowsCount'] ?? 'unknown',
                $tableInfo['dataSizeBytes'] ?? 'unknown',
            ));
            return;
        }

        $this->migrateTable($tableInfo, $config);
    }

    /**
     * @param array<string, mixed> $sourceTableInfo
     */
    private function migrateTable(array $sourceTableInfo, Config $config): void
    {
        assert(isset($sourceTableInfo['id']) && is_string($sourceTableInfo['id']));
        $tableId = $sourceTableInfo['id'];
        $this->logger->info(sprintf('Exporting table %s', $tableId));
        $file = $this->sourceClient->exportTableAsync($tableId, [
            'gzip' => true,
            'includeInternalTimestamp' => $config->preserveTimestamp(),
        ]);

        $this->processExportedTable($sourceTableInfo, $file, $config);
    }

    /**
     * @return string[]
     */
    private function getAllTables(): array
    {
        $buckets = $this->sourceClient->listBuckets();
        $listTables = [];
        foreach ($buckets as $bucket) {
            $sourceBucketTables = $this->sourceClient->listTables($bucket['id']);
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
}
