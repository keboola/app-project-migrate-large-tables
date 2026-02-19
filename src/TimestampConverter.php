<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Psr\Log\LoggerInterface;
use RuntimeException;

class TimestampConverter
{
    private const TIMESTAMP_TYPES_WITH_TIMEZONE = [
        'TIMESTAMP_LTZ',
        'TIMESTAMP_TZ',
    ];

    private const DUCKDB_BINARY = 'duckdb';

    /** @var int[] */
    private array $timestampColumnIndices;
    /** @var string[] */
    private array $columns;
    private string $sourceTimezoneStr;
    private bool $hasInternalTimestamp;

    /**
     * @param string[] $columns
     * @param array<string, array<int, array<string, string>>> $columnMetadata
     */
    public function __construct(
        array $columns,
        array $columnMetadata,
        string $sourceTimezone,
        private readonly LoggerInterface $logger,
        bool $hasInternalTimestamp = false,
    ) {
        $this->columns = array_values($columns);
        $this->sourceTimezoneStr = $sourceTimezone;
        $this->hasInternalTimestamp = $hasInternalTimestamp;
        $this->timestampColumnIndices = $this->detectTimestampColumns($this->columns, $columnMetadata);
    }

    public function hasTimestampColumns(): bool
    {
        return !empty($this->timestampColumnIndices);
    }

    public function processGzippedFile(string $filePath): void
    {
        if (!$this->hasTimestampColumns()) {
            return;
        }

        $tempPath = $filePath . '.converting';
        $sql = $this->buildDuckDbQuery($filePath, $tempPath);

        $this->logger->info(sprintf('Running DuckDB timestamp conversion on %s', basename($filePath)));

        $descriptorspec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open(
            [self::DUCKDB_BINARY, '-c', $sql],
            $descriptorspec,
            $pipes,
        );

        if (!is_resource($process)) {
            throw new RuntimeException('Failed to start DuckDB process');
        }

        fclose($pipes[0]);
        $stderr = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);

        if ($exitCode !== 0) {
            throw new RuntimeException(sprintf(
                'DuckDB timestamp conversion failed (exit code %d): %s',
                $exitCode,
                $stderr,
            ));
        }

        if (!file_exists($tempPath)) {
            throw new RuntimeException(sprintf('DuckDB output file not found: %s', $tempPath));
        }

        unlink($filePath);
        rename($tempPath, $filePath);

        $this->logger->info('DuckDB timestamp conversion completed');
    }

    /**
     * @param string[] $slicePaths
     */
    public function processGzippedSlices(array $slicePaths): void
    {
        if (!$this->hasTimestampColumns()) {
            return;
        }

        foreach ($slicePaths as $path) {
            $this->processGzippedFile($path);
        }
    }

    private function buildDuckDbQuery(string $inputPath, string $outputPath): string
    {
        $columnCount = count($this->columns);
        $offset = $this->hasInternalTimestamp ? 1 : 0;
        $totalCsvColumns = $columnCount + $offset;

        $columnDefs = [];
        for ($i = 0; $i < $totalCsvColumns; $i++) {
            $columnDefs[] = sprintf("'col_%d': 'VARCHAR'", $i);
        }

        $selectExprs = [];
        for ($i = 0; $i < $totalCsvColumns; $i++) {
            $colRef = sprintf('"col_%d"', $i);
            $dataIndex = $i - $offset;
            if ($dataIndex >= 0 && in_array($dataIndex, $this->timestampColumnIndices, true)) {
                $selectExprs[] = sprintf(
                    'CASE WHEN %s IS NOT NULL AND %s != \'\'' .
                    ' AND regexp_matches(%s, \' [+-]\\d{4}$\')' .
                    ' THEN CAST(timezone(\'UTC\',' .
                    ' regexp_replace(%s, \' ([+-])(\\d{2})(\\d{2})$\', \'\\1\\2:\\3\')::TIMESTAMPTZ) AS VARCHAR)' .
                    ' WHEN %s IS NOT NULL AND %s != \'\'' .
                    ' AND TRY_CAST(%s AS TIMESTAMP) IS NOT NULL' .
                    ' THEN CAST(timezone(\'UTC\', timezone(\'%s\', %s::TIMESTAMP)) AS VARCHAR)' .
                    ' ELSE %s END',
                    $colRef,
                    $colRef,
                    $colRef,
                    $colRef,
                    $colRef,
                    $colRef,
                    $colRef,
                    $this->sourceTimezoneStr,
                    $colRef,
                    $colRef,
                );
            } else {
                $selectExprs[] = $colRef;
            }
        }

        return sprintf(
            'INSTALL icu; LOAD icu;'
            . ' COPY (SELECT %s FROM read_csv(\'%s\', header=false, columns={%s},'
            . ' auto_detect=false, compression=\'gzip\', quote=\'"\', escape=\'"\','
            . ' null_padding=true, ignore_errors=true))'
            . ' TO \'%s\' (FORMAT CSV, HEADER false, COMPRESSION \'gzip\', QUOTE \'"\');',
            implode(', ', $selectExprs),
            addcslashes($inputPath, "'"),
            implode(', ', $columnDefs),
            addcslashes($outputPath, "'"),
        );
    }

    /**
     * @param string[] $columns
     * @param array<string, array<int, array<string, string>>> $columnMetadata
     * @return int[]
     */
    private function detectTimestampColumns(array $columns, array $columnMetadata): array
    {
        $indices = [];
        foreach ($columns as $index => $columnName) {
            $metadata = $columnMetadata[$columnName] ?? [];
            foreach ($metadata as $m) {
                if (($m['provider'] ?? '') === 'storage' && ($m['key'] ?? '') === 'KBC.datatype.type') {
                    if (in_array(strtoupper($m['value'] ?? ''), self::TIMESTAMP_TYPES_WITH_TIMEZONE, true)) {
                        $this->logger->info(sprintf(
                            'Column "%s" (index %d) is %s — will convert to UTC via DuckDB',
                            $columnName,
                            $index,
                            $m['value'],
                        ));
                        $indices[] = $index;
                    }
                    break;
                }
            }
        }
        return $indices;
    }
}
