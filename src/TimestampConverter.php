<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Psr\Log\LoggerInterface;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use RuntimeException;
use SplFileInfo;

class TimestampConverter
{
    private const TIMESTAMP_TYPES_WITH_TIMEZONE = [
        'TIMESTAMP_LTZ',
        'TIMESTAMP_TZ',
    ];

    private const DUCKDB_BINARY = 'duckdb';
    private const DUCKDB_EXTENSIONS_SOURCE = '/.duckdb';

    /** @var int[] */
    private array $timestampColumnIndices;
    /** @var string[] */
    private array $columns;
    private string $sourceTimezoneStr;

    /**
     * @param string[] $columns
     * @param array<string, array<int, array<string, string>>> $columnMetadata
     */
    public function __construct(
        array $columns,
        array $columnMetadata,
        string $sourceTimezone,
        private readonly LoggerInterface $logger,
    ) {
        $this->columns = array_values($columns);
        $this->sourceTimezoneStr = $sourceTimezone;
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

        $duckDbHome = $this->ensureDuckDbHome();

        $descriptorspec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $env = getenv();
        $env['HOME'] = $duckDbHome;

        $process = proc_open(
            [self::DUCKDB_BINARY, '-c', $sql],
            $descriptorspec,
            $pipes,
            null,
            $env,
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

        $columnDefs = [];
        for ($i = 0; $i < $columnCount; $i++) {
            $columnDefs[] = sprintf("'col_%d': 'VARCHAR'", $i);
        }

        $selectExprs = [];
        for ($i = 0; $i < $columnCount; $i++) {
            $colRef = sprintf('"col_%d"', $i);
            if (in_array($i, $this->timestampColumnIndices, true)) {
                $selectExprs[] = sprintf(
                    'CASE WHEN %s IS NOT NULL AND %s != \'\''
                    . ' THEN CAST(timezone(\'UTC\', COALESCE('
                    . ' TRY_STRPTIME(%s, \'%%Y-%%m-%%d %%H:%%M:%%S.%%g %%z\'),'
                    . ' TRY_STRPTIME(%s, \'%%Y-%%m-%%d %%H:%%M:%%S %%z\'),'
                    . ' timezone(\'%s\', TRY_CAST(%s AS TIMESTAMP))'
                    . ')) AS VARCHAR)'
                    . ' ELSE %s END',
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
            'LOAD icu;'
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

    private function ensureDuckDbHome(): string
    {
        $home = sys_get_temp_dir() . '/duckdb-home';
        $targetDir = $home . '/.duckdb';
        if (!is_dir($targetDir) && is_dir(self::DUCKDB_EXTENSIONS_SOURCE)) {
            $this->recursiveCopy(self::DUCKDB_EXTENSIONS_SOURCE, $targetDir);
        }
        if (!is_dir($home)) {
            mkdir($home, 0777, true);
        }
        return $home;
    }

    private function recursiveCopy(string $source, string $destination): void
    {
        mkdir($destination, 0777, true);
        $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($source, RecursiveDirectoryIterator::SKIP_DOTS),
            RecursiveIteratorIterator::SELF_FIRST,
        );
        foreach ($iterator as $item) {
            assert($item instanceof SplFileInfo);
            $target = $destination . '/' . $iterator->getSubPathname();
            if ($item->isDir()) {
                if (!is_dir($target)) {
                    mkdir($target, 0777, true);
                }
            } else {
                copy($item->getPathname(), $target);
            }
        }
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
