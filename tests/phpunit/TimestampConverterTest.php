<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\TimestampConverter;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TimestampConverterTest extends TestCase
{
    /**
     * @dataProvider provideTimestampConversionData
     * @param array<int, array<string, string>> $rows Each row is [col_index => value]
     * @param array<int, array<string, string>> $expectedRows
     */
    public function testTimestampConversion(
        string $sourceTimezone,
        array $rows,
        array $expectedRows,
        bool $hasInternalTimestamp = false,
    ): void {
        $columns = ['ts_col', 'other_col'];
        $columnMetadata = [
            'ts_col' => [
                ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_TZ'],
            ],
        ];

        $converter = new TimestampConverter(
            $columns,
            $columnMetadata,
            $sourceTimezone,
            new NullLogger(),
            $hasInternalTimestamp,
        );

        self::assertTrue($converter->hasTimestampColumns());

        $tmpDir = sys_get_temp_dir() . '/ts_converter_test_' . uniqid();
        mkdir($tmpDir, 0777, true);
        $inputFile = $tmpDir . '/input.csv.gz';

        $csvLines = [];
        foreach ($rows as $row) {
            $csvLines[] = $this->toCsvLine($row);
        }
        file_put_contents(
            $inputFile,
            gzencode(implode("\n", $csvLines) . "\n"),
        );

        $converter->processGzippedFile($inputFile);

        $output = gzdecode((string) file_get_contents($inputFile));
        self::assertNotFalse($output);
        $outputLines = array_filter(explode("\n", $output), fn($l) => $l !== '');

        self::assertCount(count($expectedRows), $outputLines, 'Row count mismatch');

        foreach ($expectedRows as $i => $expectedRow) {
            $actualRow = str_getcsv($outputLines[$i]);
            foreach ($expectedRow as $colIdx => $expectedValue) {
                self::assertSame(
                    $expectedValue,
                    $actualRow[$colIdx] ?? null,
                    sprintf('Row %d, col %d mismatch', $i, $colIdx),
                );
            }
        }

        $this->removeDir($tmpDir);
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    public static function provideTimestampConversionData(): array
    {
        return [
            'TIMESTAMP_TZ with embedded offset -0800 should convert to UTC' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000 -0800', 'value1'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-15 07:43:22', 1 => 'value1'],
                ],
            ],
            'TIMESTAMP_TZ with positive offset +0530 should convert to UTC' => [
                'sourceTimezone' => 'Asia/Kolkata',
                'rows' => [
                    ['2024-11-14 23:43:22.000 +0530', 'value2'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-14 18:13:22', 1 => 'value2'],
                ],
            ],
            'TIMESTAMP_TZ with UTC offset +0000' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000 +0000', 'value3'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-14 23:43:22', 1 => 'value3'],
                ],
            ],
            'plain timestamp without offset uses sourceTimezone' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000', 'value4'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-15 07:43:22', 1 => 'value4'],
                ],
            ],
            'empty and null values pass through unchanged' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['', 'value5'],
                ],
                'expectedRows' => [
                    [0 => '', 1 => 'value5'],
                ],
            ],
            'ISO format offset with colon, no space' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000000-08:00', 'value6'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-15 07:43:22', 1 => 'value6'],
                ],
            ],
            'offset with space and colon' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000 -08:00', 'value7'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-15 07:43:22', 1 => 'value7'],
                ],
            ],
            'offset without space and without colon' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000-0800', 'value8'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-15 07:43:22', 1 => 'value8'],
                ],
            ],
            'multiple rows with mixed formats' => [
                'sourceTimezone' => 'America/Los_Angeles',
                'rows' => [
                    ['2024-11-14 23:43:22.000 -0800', 'row1'],
                    ['2024-06-15 10:00:00.000 -0700', 'row2'],
                    ['2024-01-01 00:00:00.000', 'row3'],
                    ['2024-11-14 23:43:22.000000-08:00', 'row4'],
                ],
                'expectedRows' => [
                    [0 => '2024-11-15 07:43:22', 1 => 'row1'],
                    [0 => '2024-06-15 17:00:00', 1 => 'row2'],
                    [0 => '2024-01-01 08:00:00', 1 => 'row3'],
                    [0 => '2024-11-15 07:43:22', 1 => 'row4'],
                ],
            ],
        ];
    }

    /**
     * @param string[] $values
     */
    private function toCsvLine(array $values): string
    {
        $escaped = array_map(function (string $v): string {
            if ($v === '') {
                return '""';
            }
            return '"' . str_replace('"', '""', $v) . '"';
        }, $values);
        return implode(',', $escaped);
    }

    private function removeDir(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $items = scandir($dir);
        if ($items === false) {
            return;
        }
        foreach ($items as $item) {
            if ($item === '.' || $item === '..') {
                continue;
            }
            $path = $dir . '/' . $item;
            if (is_dir($path)) {
                $this->removeDir($path);
            } else {
                unlink($path);
            }
        }
        rmdir($dir);
    }
}
