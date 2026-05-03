<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Strategy;

use GuzzleHttp\Client as GuzzleClient;
use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\MigrateInterface;
use Keboola\AppProjectMigrateLargeTables\StorageModifier;
use Keboola\AppProjectMigrateLargeTables\Strategy\SapiMigrate\MigrateGcsLargeTable;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;

class SapiMigrate implements MigrateInterface
{
    private const LARGE_GCS_TABLE_SIZE = 50*1000*1000*1000; // 50 GB
    private StorageModifier $storageModifier;
    private MigrateGcsLargeTable $migrateGcsLargeTable;

    /** @var string[] $bucketsExist */
    private array $bucketsExist = [];

    /** @var array<string, string> $destinationBackendCache */
    private array $destinationBackendCache = [];

    private ?int $workspaceId = null;
    private ?string $workspaceBackend = null;
    private ?int $defaultBranchId = null;

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
        try {
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
        } finally {
            $this->cleanupWorkspace();
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
     * For incremental migration, resolve the changedSince parameter.
     * If the user provided an explicit changedSince value, use that.
     * Otherwise, query the max _timestamp from the target table.
     */
    private function resolveChangedSince(array $sourceTableInfo, Config $config): ?string
    {
        if (!$config->isIncremental()) {
            return null;
        }

        $userChangedSince = $config->getChangedSince();
        if ($userChangedSince !== null) {
            return $userChangedSince;
        }

        if (!$this->targetClient->tableExists($sourceTableInfo['id'])) {
            return null;
        }

        $targetTableInfo = $this->targetClient->getTable($sourceTableInfo['id']);
        if (($targetTableInfo['rowsCount'] ?? 0) === 0) {
            return null;
        }

        try {
            return $this->getMaxTimestamp($sourceTableInfo['id']);
        } catch (ClientException $e) {
            $this->logger->warning(sprintf(
                'Cannot resolve changedSince for table %s, falling back to full export: %s',
                $sourceTableInfo['id'],
                $e->getMessage(),
            ));
            return null;
        }
    }

    /**
     * Get the max _timestamp value from a target table using the Query Service API.
     * Creates a read-only workspace lazily and reuses it across tables.
     */
    private function getMaxTimestamp(string $tableId): ?string
    {
        $workspaceId = $this->getOrCreateWorkspace();

        // Table ID format: "stage.c-bucket.tableName" -> schema "stage.c-bucket", table "tableName"
        $parts = explode('.', $tableId);
        $tableName = array_pop($parts);
        $schemaName = implode('.', $parts);

        // BigQuery dataset names: dots and hyphens replaced with underscores
        if ($this->workspaceBackend === 'bigquery') {
            $schemaName = str_replace(['.', '-'], '_', $schemaName);
        }

        $sql = sprintf(
            'SELECT MAX(%s) FROM %s.%s',
            $this->quoteIdentifier('_timestamp'),
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName),
        );

        $result = $this->executeQueryViaQueryService($workspaceId, $sql);

        // Result format: {"columns": [...], "data": [["value"]], ...}
        $maxTimestamp = $result['data'][0][0] ?? null;
        if ($maxTimestamp === null || $maxTimestamp === '') {
            return null;
        }

        return $maxTimestamp;
    }

    /**
     * Execute a SQL query via the Keboola Query Service API.
     * Submits a query job, polls for completion, and returns results.
     */
    private function executeQueryViaQueryService(int $workspaceId, string $sql): array
    {
        $queryServiceUrl = $this->targetClient->getServiceUrl('query');
        $token = $this->targetClient->token;

        $httpClient = new GuzzleClient([
            'base_uri' => rtrim($queryServiceUrl, '/') . '/',
            'headers' => [
                'X-StorageApi-Token' => $token,
                'Content-Type' => 'application/json',
            ],
        ]);

        // Submit query job
        $branchId = $this->getDefaultBranchId();
        $submitResponse = $httpClient->post(
            sprintf('api/v1/branches/%d/workspaces/%d/queries', $branchId, $workspaceId),
            [
                'json' => [
                    'statements' => [$sql],
                    'transactional' => false,
                ],
            ],
        );

        /** @var array{queryJobId?: string} $submitResult */
        $submitResult = json_decode((string) $submitResponse->getBody(), true);
        $queryJobId = $submitResult['queryJobId'] ?? null;
        if ($queryJobId === null) {
            throw new RuntimeException('Query Service did not return a queryJobId');
        }

        // Poll for job completion
        $maxAttempts = 60;
        $statementId = null;
        for ($i = 0; $i < $maxAttempts; $i++) {
            usleep(500000); // 500ms

            $statusResponse = $httpClient->get(sprintf('api/v1/queries/%s', $queryJobId));
            /** @var array{status?: string, statements?: list<array{id?: string, error?: string}>} $statusResult */
            $statusResult = json_decode((string) $statusResponse->getBody(), true);
            $status = (string) ($statusResult['status'] ?? 'unknown');

            if ($status === 'completed') {
                $statementId = (string) ($statusResult['statements'][0]['id'] ?? '');
                break;
            }

            if ($status === 'failed' || $status === 'canceled') {
                $error = (string) ($statusResult['statements'][0]['error'] ?? 'Unknown error');
                throw new RuntimeException(sprintf(
                    'Query Service job %s %s: %s',
                    $queryJobId,
                    $status,
                    $error,
                ));
            }
        }

        if ($statementId === null || $statementId === '') {
            throw new RuntimeException(sprintf('Query Service job %s did not complete in time', $queryJobId));
        }

        // Get results
        $resultResponse = $httpClient->get(
            sprintf('api/v1/queries/%s/%s/results', $queryJobId, $statementId),
        );

        $resultData = json_decode((string) $resultResponse->getBody(), true);
        if (!is_array($resultData)) {
            throw new RuntimeException('Query Service returned invalid results');
        }

        return $resultData;
    }

    private function getOrCreateWorkspace(): int
    {
        if ($this->workspaceId !== null) {
            return $this->workspaceId;
        }

        $this->logger->info('Creating read-only workspace for _timestamp queries');
        $workspaces = new Workspaces($this->targetClient);
        $workspace = $workspaces->createWorkspace([
            'readOnlyStorageAccess' => true,
        ]);
        $this->workspaceId = (int) $workspace['id'];
        $this->workspaceBackend = (string) ($workspace['connection']['backend'] ?? 'snowflake');

        return $this->workspaceId;
    }

    private function getDefaultBranchId(): int
    {
        if ($this->defaultBranchId !== null) {
            return $this->defaultBranchId;
        }

        $devBranches = new DevBranches($this->targetClient);
        $defaultBranch = $devBranches->getDefaultBranch();
        $this->defaultBranchId = (int) $defaultBranch['id'];

        return $this->defaultBranchId;
    }

    private function cleanupWorkspace(): void
    {
        if ($this->workspaceId === null) {
            return;
        }

        try {
            $this->logger->info('Cleaning up read-only workspace');
            $workspaces = new Workspaces($this->targetClient);
            $workspaces->deleteWorkspace($this->workspaceId);
        } catch (Throwable $e) {
            $this->logger->warning(sprintf('Failed to delete workspace: %s', $e->getMessage()));
        } finally {
            $this->workspaceId = null;
        }
    }

    private function quoteIdentifier(string $identifier): string
    {
        if ($this->workspaceBackend === 'bigquery') {
            return '`' . str_replace('`', '\`', $identifier) . '`';
        }
        return '"' . str_replace('"', '""', $identifier) . '"';
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
