<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use DateTime;
use DateTimeZone;
use Psr\Log\LoggerInterface;
use RuntimeException;

class TimestampConverter
{
    private const TIMESTAMP_TYPES_WITH_TIMEZONE = [
        'TIMESTAMP_LTZ',
    ];

    /** @var int[] */
    private array $timestampColumnIndices;
    private DateTimeZone $sourceTimezone;
    private DateTimeZone $utcTimezone;

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
        $this->sourceTimezone = new DateTimeZone($sourceTimezone);
        $this->utcTimezone = new DateTimeZone('UTC');
        $this->timestampColumnIndices = $this->detectTimestampColumns($columns, $columnMetadata);
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

        $input = fopen('compress.zlib://' . $filePath, 'r');
        if ($input === false) {
            throw new RuntimeException(sprintf('Cannot open file for reading: %s', $filePath));
        }

        $output = gzopen($tempPath, 'wb');
        if ($output === false) {
            fclose($input);
            throw new RuntimeException(sprintf('Cannot open file for writing: %s', $tempPath));
        }

        $csvOutput = fopen('php://temp', 'w+');
        if ($csvOutput === false) {
            fclose($input);
            gzclose($output);
            throw new RuntimeException('Cannot open temp stream for CSV writing');
        }

        $rowCount = 0;
        while (($row = fgetcsv($input, 0, ',', '"', '\\')) !== false) {
            foreach ($this->timestampColumnIndices as $idx) {
                if (isset($row[$idx]) && $row[$idx] !== '') {
                    $row[$idx] = $this->convertTimestamp($row[$idx]);
                }
            }
            rewind($csvOutput);
            ftruncate($csvOutput, 0);
            fputcsv($csvOutput, $row, ',', '"', '\\');
            rewind($csvOutput);
            $csvLine = stream_get_contents($csvOutput);
            if ($csvLine !== false) {
                gzwrite($output, $csvLine);
            }
            $rowCount++;
        }

        fclose($csvOutput);
        fclose($input);
        gzclose($output);

        unlink($filePath);
        rename($tempPath, $filePath);

        $this->logger->info(sprintf('Converted timestamps to UTC in %d rows', $rowCount));
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
                            'Column "%s" (index %d) is %s — will convert to UTC',
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

    private function convertTimestamp(string $value): string
    {
        $dt = DateTime::createFromFormat('Y-m-d H:i:s.u', $value, $this->sourceTimezone);
        if ($dt !== false) {
            $dt->setTimezone($this->utcTimezone);
            return $dt->format('Y-m-d H:i:s.u');
        }

        $dt = DateTime::createFromFormat('Y-m-d H:i:s', $value, $this->sourceTimezone);
        if ($dt !== false) {
            $dt->setTimezone($this->utcTimezone);
            return $dt->format('Y-m-d H:i:s');
        }

        throw new RuntimeException(sprintf('Cannot parse timestamp value: %s', $value));
    }
}
