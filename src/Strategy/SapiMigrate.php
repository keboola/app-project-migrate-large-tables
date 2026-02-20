<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate\MigrateGcsLargeTable;
use Keboola\AppProjectMigrateLargeTables\TimestampConverter;
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
        private readonly string $sourceTimezone = 'America/Los_Angeles',
    ) {
        $this->storageModifier = new StorageModifier($this->targetClient);
        $this->migrateGcsLargeTable = new MigrateGcsLargeTable(
            $this->sourceClient,
            $this->targetClient,
            $this->logger,
            $this->dryRun,
            $this->sourceTimezone,
        );
    }

    public function migrate(Config $config): void
    {
        $parallelism = $config->getParallelism();
        $tablesToMigrate = $this->prepareTablesToMigrate($config);
        if ($parallelism <= 1) {
            foreach ($tablesToMigrate as $tableInfo) {
                $this->migrateTable($tableInfo, $config);
            }
        } else {
            $this->logger->info(sprintf('Migrating tables with parallelism: %d', $parallelism));
            $this->migrateTablesInParallel($tablesToMigrate, $config, $parallelism);
        }
    }

    /**
     * Prepares tables for migration by validating them and creating buckets/tables as needed.
     * @return array<array<string, mixed>> Array of table info arrays ready for migration
     */
    private function prepareTablesToMigrate(Config $config): array
    {
        $tablesToMigrate = [];
        foreach ($config->getMigrateTables() ?: $this->getAllTables() as $tableId) {
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
                    $this->storageModifier->createTable($tableInfo);
                }
            }
            $tablesToMigrate[] = $tableInfo;
        }
        return $tablesToMigrate;
    }

    /**
     * Migrates tables in parallel batches.
     * @param array<array<string, mixed>> $tablesToMigrate
     */
    private function migrateTablesInParallel(array $tablesToMigrate, Config $config, int $parallelism): void
    {
        $batches = array_chunk($tablesToMigrate, max(1, $parallelism));
        foreach ($batches as $batchIndex => $batch) {
            $this->logger->info(sprintf(
                'Processing batch %d/%d (%d tables)',
                $batchIndex + 1,
                count($batches),
                count($batch),
            ));
            $this->processBatch($batch, $config);
        }
    }

    /**
     * Processes a batch of tables in parallel.
     * @param array<array<string, mixed>> $batch
     */
    private function processBatch(array $batch, Config $config): void
    {
        $exportJobs = [];
        foreach ($batch as $tableInfo) {
            assert(is_string($tableInfo['id']));
            $tableId = $tableInfo['id'];
            $this->logger->info(sprintf('Queueing export for table %s', $tableId));
            $jobId = $this->sourceClient->queueTableExport($tableId, [
                'gzip' => true,
                'includeInternalTimestamp' => $config->preserveTimestamp(),
            ]);
            $exportJobs[$tableId] = [
                'jobId' => $jobId,
                'tableInfo' => $tableInfo,
            ];
        }
        $exportResults = [];
        foreach ($exportJobs as $tableId => $jobData) {
            $this->logger->info(sprintf('Waiting for export job for table %s', $tableId));
            $job = $this->sourceClient->waitForJob($jobData['jobId']);
            if ($job === null || $job['status'] !== 'success') {
                $errorMessage = $job['error']['message'] ?? 'Unknown error';
                $this->logger->warning(sprintf(
                    'Export failed for table %s: %s',
                    $tableId,
                    $errorMessage,
                ));
                continue;
            }
            $results = $job['results'];
            assert(is_array($results) && is_array($results['file']));
            $file = $results['file'];
            assert(isset($file['id']) && is_int($file['id']));
            $fileId = $file['id'];
            $exportResults[$tableId] = [
                'tableInfo' => $jobData['tableInfo'],
                'fileId' => $fileId,
            ];
        }
        $uploadResults = [];
        foreach ($exportResults as $tableId => $exportData) {
            $result = $this->downloadAndUpload($exportData['tableInfo'], $exportData['fileId'], $config);
            if ($result !== null) {
                $uploadResults[$tableId] = [
                    'tableInfo' => $exportData['tableInfo'],
                    'destinationFileId' => $result,
                ];
            }
        }
        $importJobs = [];
        foreach ($uploadResults as $tableId => $uploadData) {
            if ($this->dryRun) {
                assert(is_string($uploadData['tableInfo']['name']));
                $tableName = $uploadData['tableInfo']['name'];
                $this->logger->info(sprintf('[dry-run] Import data to table "%s"', $tableName));
                continue;
            }
            $this->logger->info(sprintf('Queueing import for table %s', $tableId));
            $jobId = $this->targetClient->queueTableImport(
                $tableId,
                [
                    'name' => $uploadData['tableInfo']['name'],
                    'dataFileId' => $uploadData['destinationFileId'],
                    'columns' => $uploadData['tableInfo']['columns'],
                    'useTimestampFromDataFile' => $config->preserveTimestamp(),
                ],
            );
            $importJobs[$tableId] = $jobId;
        }
        foreach ($importJobs as $tableId => $jobId) {
            $this->logger->info(sprintf('Waiting for import job for table %s', $tableId));
            $job = $this->targetClient->waitForJob($jobId);
            if ($job === null || $job['status'] !== 'success') {
                $errorMessage = $job['error']['message'] ?? 'Unknown error';
                $this->logger->warning(sprintf(
                    'Import failed for table %s: %s',
                    $tableId,
                    $errorMessage,
                ));
            }
        }
    }

    /**
     * Downloads file from source and uploads to target.
     * @param array<string, mixed> $tableInfo
     * @return int|null The destination file ID, or null if dry run or large GCS table
     */
    private function downloadAndUpload(array $tableInfo, int $sourceFileId, Config $config): ?int
    {
        $sourceFileInfo = $this->sourceClient->getFile($sourceFileId);
        $tmp = new Temp();
        $optionUploadedFile = new FileUploadOptions();
        assert(is_string($tableInfo['id']));
        $tableIdStr = $tableInfo['id'];
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($tableIdStr);
        $tableSize = $sourceFileInfo['sizeBytes'];
        if ($sourceFileInfo['provider'] === 'gcp' &&
            $sourceFileInfo['isSliced'] === true &&
            $tableSize > self::LARGE_GCS_TABLE_SIZE
        ) {
            $this->migrateGcsLargeTable->migrate(
                $sourceFileId,
                $tableInfo,
                $config->preserveTimestamp(),
                $tmp,
            );
            $tmp->remove();
            return null;
        } elseif ($sourceFileInfo['isSliced'] === true) {
            $optionUploadedFile->setIsSliced(true);
            if ($this->dryRun === false) {
                $this->logger->info(sprintf('Downloading table %s', $tableIdStr));
                $slices = $this->sourceClient->downloadSlicedFile($sourceFileId, $tmp->getTmpFolder());
                $this->convertTimestampsInSlices($tableInfo, $slices);
                $this->logger->info(sprintf('Uploading table %s', $tableIdStr));
                $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);
            } else {
                $this->logger->info(sprintf('[dry-run] Migrate table %s', $tableIdStr));
                $tmp->remove();
                return null;
            }
        } else {
            $fileName = $tmp->getTmpFolder() . '/' . $sourceFileInfo['name'];
            if ($this->dryRun === false) {
                $this->logger->info(sprintf('Downloading table %s', $tableIdStr));
                $this->sourceClient->downloadFile($sourceFileId, $fileName);
                $this->convertTimestampsInFile($tableInfo, $fileName);
                $this->logger->info(sprintf('Uploading table %s', $tableIdStr));
                $destinationFileId = $this->targetClient->uploadFile($fileName, $optionUploadedFile);
            } else {
                $this->logger->info(sprintf('[dry-run] Uploading table %s', $tableIdStr));
                $tmp->remove();
                return null;
            }
        }
        $tmp->remove();
        return $destinationFileId;
    }

    private function migrateTable(array $sourceTableInfo, Config $config): void
    {
        if ($this->dryRun) {
            $this->logger->info(sprintf('[dry-run] Migrate table %s', $sourceTableInfo['id']));
            return;
        }

        $this->logger->info(sprintf('Exporting table %s', $sourceTableInfo['id']));
        $file = $this->sourceClient->exportTableAsync($sourceTableInfo['id'], [
            'gzip' => true,
            'includeInternalTimestamp' => $config->preserveTimestamp(),
        ]);

        $sourceFileId = $file['file']['id'];
        $sourceFileInfo = $this->sourceClient->getFile($sourceFileId);

        $tmp = new Temp();
        $optionUploadedFile = new FileUploadOptions();
        $optionUploadedFile
            ->setFederationToken(true)
            ->setFileName($sourceTableInfo['id'])
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

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $slices = $this->sourceClient->downloadSlicedFile($sourceFileId, $tmp->getTmpFolder());
            $this->convertTimestampsInSlices($sourceTableInfo, $slices);

            $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
            $destinationFileId = $this->targetClient->uploadSlicedFile($slices, $optionUploadedFile);
        } else {
            $fileName = $tmp->getTmpFolder() . '/' . $sourceFileInfo['name'];

            $this->logger->info(sprintf('Downloading table %s', $sourceTableInfo['id']));
            $this->sourceClient->downloadFile($sourceFileId, $fileName);
            $this->convertTimestampsInFile($sourceTableInfo, $fileName);

            $this->logger->info(sprintf('Uploading table %s', $sourceTableInfo['id']));
            $destinationFileId = $this->targetClient->uploadFile($fileName, $optionUploadedFile);
        }

        // Upload data to table
        $this->targetClient->writeTableAsyncDirect(
            $sourceTableInfo['id'],
            [
                'name' => $sourceTableInfo['name'],
                'dataFileId' => $destinationFileId,
                'columns' => $sourceTableInfo['columns'],
                'useTimestampFromDataFile' => $config->preserveTimestamp(),
            ],
        );

        $tmp->remove();
    }

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

    /**
     * @param array<string, mixed> $tableInfo
     */
    private function createTimestampConverter(array $tableInfo): TimestampConverter
    {
        /** @var string[] $columns */
        $columns = $tableInfo['columns'];
        /** @var array<string, array<int, array<string, string>>> $columnMetadata */
        $columnMetadata = $tableInfo['columnMetadata'] ?? [];
        return new TimestampConverter(
            $columns,
            $columnMetadata,
            $this->sourceTimezone,
            $this->logger,
        );
    }

    /**
     * @param array<string, mixed> $tableInfo
     * @param string[] $slicePaths
     */
    private function convertTimestampsInSlices(array $tableInfo, array $slicePaths): void
    {
        $converter = $this->createTimestampConverter($tableInfo);
        if ($converter->hasTimestampColumns()) {
            assert(is_string($tableInfo['id']));
            $this->logger->info(sprintf('Converting timezone timestamps to UTC for table %s', $tableInfo['id']));
            $converter->processGzippedSlices($slicePaths);
        }
    }

    /**
     * @param array<string, mixed> $tableInfo
     */
    private function convertTimestampsInFile(array $tableInfo, string $filePath): void
    {
        $converter = $this->createTimestampConverter($tableInfo);
        if ($converter->hasTimestampColumns()) {
            assert(is_string($tableInfo['id']));
            $this->logger->info(sprintf('Converting timezone timestamps to UTC for table %s', $tableInfo['id']));
            $converter->processGzippedFile($filePath);
        }
    }
}
