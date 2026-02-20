<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\TimestampConverter;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class TimestampConverterTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        $this->tempDir = sys_get_temp_dir() . '/ts-converter-test-' . uniqid();
        mkdir($this->tempDir, 0777, true);
    }

    protected function tearDown(): void
    {
        $files = glob($this->tempDir . '/*');
        if ($files !== false) {
            foreach ($files as $file) {
                unlink($file);
            }
        }
        rmdir($this->tempDir);
    }

    public function testNoTimestampColumnsSkipsProcessing(): void
    {
        $converter = new TimestampConverter(
            ['id', 'name'],
            [
                'id' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'INTEGER']],
                'name' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR']],
            ],
            'America/Los_Angeles',
            new NullLogger(),
        );

        self::assertFalse($converter->hasTimestampColumns());
    }

    public function testDetectsTimestampLtzColumn(): void
    {
        $converter = new TimestampConverter(
            ['id', 'created_at'],
            [
                'id' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'INTEGER']],
                'created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']],
            ],
            'America/Los_Angeles',
            new NullLogger(),
        );

        self::assertTrue($converter->hasTimestampColumns());
    }

    public function testDetectsTimestampTzColumn(): void
    {
        $converter = new TimestampConverter(
            ['id', 'updated_at'],
            [
                'id' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'INTEGER']],
                'updated_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_TZ']],
            ],
            'America/Los_Angeles',
            new NullLogger(),
        );

        self::assertTrue($converter->hasTimestampColumns());
    }

    public function testSnowflakeOffsetFormat(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000 -0800', 'hello'],
        ]);

        $converter = $this->createConverter(['id', 'ts', 'name'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('1', $rows[0][0]);
        self::assertSame('2024-11-15 07:43:22', $rows[0][1]);
        self::assertSame('hello', $rows[0][2]);
    }

    public function testPlainTimestampUsesSourceTimezone(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000', 'world'],
        ]);

        $converter = $this->createConverter(
            ['id', 'ts', 'name'],
            'ts',
            'TIMESTAMP_LTZ',
            'America/Los_Angeles',
        );
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('2024-11-15 07:43:22', $rows[0][1]);
    }

    public function testPositiveOffset(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000 +0530'],
        ]);

        $converter = $this->createConverter(['id', 'ts'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('2024-11-14 18:13:22', $rows[0][1]);
    }

    public function testUtcOffset(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000 +0000'],
        ]);

        $converter = $this->createConverter(['id', 'ts'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('2024-11-14 23:43:22', $rows[0][1]);
    }

    public function testSixDigitMicroseconds(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000000 -0800', 'hello'],
        ]);

        $converter = $this->createConverter(['id', 'ts', 'name'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('2024-11-15 07:43:22', $rows[0][1]);
    }

    public function testNoFractionalSecondsWithOffset(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22 -0800'],
        ]);

        $converter = $this->createConverter(['id', 'ts'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('2024-11-15 07:43:22', $rows[0][1]);
    }

    public function testEmptyValuePreserved(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '', 'test'],
        ]);

        $converter = $this->createConverter(['id', 'ts', 'name'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('', $rows[0][1]);
    }

    public function testMultipleRowsMixedFormats(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000 -0800', 'row1'],
            ['2', '2024-06-15 10:00:00.000 +0000', 'row2'],
            ['3', '', 'row3'],
            ['4', '2024-01-01 12:00:00.000 +0530', 'row4'],
        ]);

        $converter = $this->createConverter(['id', 'ts', 'name'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(4, $rows);
        self::assertSame('2024-11-15 07:43:22', $rows[0][1]);
        self::assertSame('2024-06-15 10:00:00', $rows[1][1]);
        self::assertSame('', $rows[2][1]);
        self::assertSame('2024-01-01 06:30:00', $rows[3][1]);
    }

    public function testNonTimestampColumnsUnchanged(): void
    {
        $filePath = $this->createGzippedCsv([
            ['some-id-123', '2024-11-14 23:43:22.000 -0800', 'text with, comma'],
        ]);

        $converter = $this->createConverter(['id', 'ts', 'desc'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertSame('some-id-123', $rows[0][0]);
        self::assertSame('text with, comma', $rows[0][2]);
    }

    public function testExtraColumnsInCsvBeyondMetadata(): void
    {
        $filePath = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000 -0800', 'hello', 'extra_value'],
        ]);

        $converter = $this->createConverter(['id', 'ts', 'name'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedFile($filePath);

        $rows = $this->readGzippedCsv($filePath);
        self::assertCount(1, $rows);
        self::assertCount(4, $rows[0]);
        self::assertSame('2024-11-15 07:43:22', $rows[0][1]);
        self::assertSame('hello', $rows[0][2]);
        self::assertSame('extra_value', $rows[0][3]);
    }

    public function testProcessGzippedSlices(): void
    {
        $file1 = $this->createGzippedCsv([
            ['1', '2024-11-14 23:43:22.000 -0800'],
        ]);
        $file2 = $this->createGzippedCsv([
            ['2', '2024-06-15 10:00:00.000 +0000'],
        ]);

        $converter = $this->createConverter(['id', 'ts'], 'ts', 'TIMESTAMP_LTZ');
        $converter->processGzippedSlices([$file1, $file2]);

        $rows1 = $this->readGzippedCsv($file1);
        $rows2 = $this->readGzippedCsv($file2);
        self::assertSame('2024-11-15 07:43:22', $rows1[0][1]);
        self::assertSame('2024-06-15 10:00:00', $rows2[0][1]);
    }

    private function createConverter(
        array $columns,
        string $tsColumn,
        string $tsType,
        string $timezone = 'America/Los_Angeles',
    ): TimestampConverter {
        $metadata = [];
        foreach ($columns as $col) {
            if ($col === $tsColumn) {
                $metadata[$col] = [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => $tsType]];
            } else {
                $metadata[$col] = [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR']];
            }
        }
        return new TimestampConverter($columns, $metadata, $timezone, new NullLogger());
    }

    /**
     * @param array<int, array<int, string>> $rows
     */
    private function createGzippedCsv(array $rows): string
    {
        $filePath = $this->tempDir . '/test-' . uniqid() . '.csv.gz';
        $gz = gzopen($filePath, 'wb');
        assert($gz !== false);
        foreach ($rows as $row) {
            $parts = [];
            foreach ($row as $field) {
                if (str_contains($field, ',') || str_contains($field, '"') || str_contains($field, "\n")) {
                    $parts[] = '"' . str_replace('"', '""', $field) . '"';
                } else {
                    $parts[] = $field;
                }
            }
            gzwrite($gz, implode(',', $parts) . "\n");
        }
        gzclose($gz);
        return $filePath;
    }

    /**
     * @return array<int, array<int, string>>
     */
    private function readGzippedCsv(string $filePath): array
    {
        $input = fopen('compress.zlib://' . $filePath, 'r');
        assert($input !== false);
        $rows = [];
        while (($row = fgetcsv($input, 0, ',', '"', '\\')) !== false) {
            $rows[] = $row;
        }
        fclose($input);
        return $rows;
    }
}
