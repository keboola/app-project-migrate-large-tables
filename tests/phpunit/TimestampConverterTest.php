<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\TimestampConverter;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use RuntimeException;

class TimestampConverterTest extends TestCase
{
    public function testDetectsTimestampLtzColumns(): void
    {
        $converter = new TimestampConverter(
            ['id', 'name', 'created_at'],
            [
                'created_at' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ'],
                ],
            ],
            'America/Los_Angeles',
            new NullLogger(),
        );
        $this->assertTrue($converter->hasTimestampColumns());
    }

    public function testIgnoresTimestampTzColumns(): void
    {
        $converter = new TimestampConverter(
            ['id', 'name', 'created_at'],
            [
                'created_at' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_TZ'],
                ],
            ],
            'America/Los_Angeles',
            new NullLogger(),
        );
        $this->assertFalse($converter->hasTimestampColumns());
    }

    public function testIgnoresTimestampNtzColumns(): void
    {
        $converter = new TimestampConverter(
            ['id', 'created_at'],
            [
                'created_at' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_NTZ'],
                ],
            ],
            'UTC',
            new NullLogger(),
        );
        $this->assertFalse($converter->hasTimestampColumns());
    }

    public function testNoTimestampColumnsDetected(): void
    {
        $converter = new TimestampConverter(
            ['id', 'name'],
            [
                'id' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'INTEGER'],
                ],
                'name' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'VARCHAR'],
                ],
            ],
            'UTC',
            new NullLogger(),
        );
        $this->assertFalse($converter->hasTimestampColumns());
    }

    public function testConvertTimestampWithFractionalSeconds(): void
    {
        $inputCsv = '1,"hello","2024-02-13 00:22:47.000"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'name', 'created_at'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'America/Los_Angeles',
            '1,hello,"2024-02-13 08:22:47.000000"' . "\n",
        );
    }

    public function testConvertTimestampWithoutFractionalSeconds(): void
    {
        $inputCsv = '1,"hello","2024-02-13 00:22:47"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'name', 'created_at'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'America/Los_Angeles',
            '1,hello,"2024-02-13 08:22:47"' . "\n",
        );
    }

    public function testConvertTimestampFromUtcIsNoop(): void
    {
        $inputCsv = '1,"2024-02-13 08:22:47.000"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'created_at'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'UTC',
            '1,"2024-02-13 08:22:47.000000"' . "\n",
        );
    }

    public function testThrowsOnUnparseableTimestamp(): void
    {
        $inputCsv = '1,"not-a-timestamp"' . "\n";
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot parse timestamp value: not-a-timestamp');

        $this->runConversion(
            $inputCsv,
            ['id', 'created_at'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'UTC',
        );
    }

    public function testSkipsEmptyTimestampValues(): void
    {
        $inputCsv = '1,""' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'created_at'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'UTC',
            '1,' . "\n",
        );
    }

    public function testCsvRoundTripWithSpecialCharacters(): void
    {
        $inputCsv = '"field,with,commas","field""with""quotes","2024-01-15 10:00:00.000"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['col1', 'col2', 'ts'],
            ['ts' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'UTC',
            '"field,with,commas","field""with""quotes","2024-01-15 10:00:00.000000"' . "\n",
        );
    }

    public function testCsvRoundTripWithBackslashes(): void
    {
        $inputCsv = '"path\\to\\file","2024-01-15 10:00:00.000"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['path', 'ts'],
            ['ts' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'UTC',
            '"path\\to\\file","2024-01-15 10:00:00.000000"' . "\n",
        );
    }

    public function testProcessGzippedFileNoTimestampColumns(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'ts_test_');
        assert($tmpFile !== false);
        $gz = gzopen($tmpFile, 'wb');
        assert($gz !== false);
        gzwrite($gz, '1,hello' . "\n");
        gzclose($gz);

        $converter = new TimestampConverter(
            ['id', 'name'],
            [
                'id' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'INTEGER']],
            ],
            'UTC',
            new NullLogger(),
        );
        $converter->processGzippedFile($tmpFile);

        $input = fopen('compress.zlib://' . $tmpFile, 'r');
        assert($input !== false);
        $content = stream_get_contents($input);
        fclose($input);

        $this->assertEquals('1,hello' . "\n", $content);
        unlink($tmpFile);
    }

    public function testProcessGzippedFileWithTimestampConversion(): void
    {
        $tmpFile = tempnam(sys_get_temp_dir(), 'ts_test_');
        assert($tmpFile !== false);
        $gz = gzopen($tmpFile, 'wb');
        assert($gz !== false);
        gzwrite($gz, '1,"2024-02-13 00:22:47.000"' . "\n");
        gzwrite($gz, '2,"2024-06-15 12:00:00.000"' . "\n");
        gzclose($gz);

        $converter = new TimestampConverter(
            ['id', 'created_at'],
            [
                'created_at' => [
                    ['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ'],
                ],
            ],
            'America/Los_Angeles',
            new NullLogger(),
        );
        $converter->processGzippedFile($tmpFile);

        $input = fopen('compress.zlib://' . $tmpFile, 'r');
        assert($input !== false);
        $content = stream_get_contents($input);
        assert(is_string($content));
        fclose($input);

        $lines = explode("\n", trim($content));
        $this->assertCount(2, $lines);
        $this->assertEquals('1,"2024-02-13 08:22:47.000000"', $lines[0]);
        $this->assertEquals('2,"2024-06-15 19:00:00.000000"', $lines[1]);

        unlink($tmpFile);
    }

    public function testProcessGzippedSlices(): void
    {
        $tmpFile1 = tempnam(sys_get_temp_dir(), 'ts_slice1_');
        $tmpFile2 = tempnam(sys_get_temp_dir(), 'ts_slice2_');
        assert($tmpFile1 !== false && $tmpFile2 !== false);

        $gz1 = gzopen($tmpFile1, 'wb');
        assert($gz1 !== false);
        gzwrite($gz1, '1,"2024-01-01 00:00:00.000"' . "\n");
        gzclose($gz1);

        $gz2 = gzopen($tmpFile2, 'wb');
        assert($gz2 !== false);
        gzwrite($gz2, '2,"2024-06-01 12:00:00.000"' . "\n");
        gzclose($gz2);

        $converter = new TimestampConverter(
            ['id', 'ts'],
            ['ts' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'America/Los_Angeles',
            new NullLogger(),
        );
        $converter->processGzippedSlices([$tmpFile1, $tmpFile2]);

        $input1 = fopen('compress.zlib://' . $tmpFile1, 'r');
        assert($input1 !== false);
        $raw1 = stream_get_contents($input1);
        assert(is_string($raw1));
        $content1 = trim($raw1);
        fclose($input1);

        $input2 = fopen('compress.zlib://' . $tmpFile2, 'r');
        assert($input2 !== false);
        $raw2 = stream_get_contents($input2);
        assert(is_string($raw2));
        $content2 = trim($raw2);
        fclose($input2);

        $this->assertEquals('1,"2024-01-01 08:00:00.000000"', $content1);
        $this->assertEquals('2,"2024-06-01 19:00:00.000000"', $content2);

        unlink($tmpFile1);
        unlink($tmpFile2);
    }

    public function testPreserveTimestampExtraColumnPassesThrough(): void
    {
        $inputCsv = '1,"2024-02-13 00:22:47.000","hello","2025-09-05 08:03:32"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'created_at', 'name'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'America/Los_Angeles',
            '1,"2024-02-13 08:22:47.000000",hello,"2025-09-05 08:03:32"' . "\n",
        );
    }

    public function testCsvWithYamlLikeContentAndExtraTimestampColumn(): void
    {
        $yaml = '--- shop_id: 4d9ed867 url: https://api.example.com/data id: 94b2964a';
        $inputCsv = '"5cc18522","StatisticsReport","update",,"' . $yaml
            . '",,"2024-05-28 14:13:30","2025-09-05 08:03:32"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'type', 'action', 'detail', 'metadata', 'extra', 'created_at'],
            ['created_at' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']]],
            'America/Los_Angeles',
            '5cc18522,StatisticsReport,update,,"' . $yaml . '",,"2024-05-28 21:13:30","2025-09-05 08:03:32"' . "\n",
        );
    }

    public function testMultipleTimestampColumnsInSameRow(): void
    {
        $inputCsv = '1,"2024-01-15 10:00:00.000","hello","2024-06-15 14:00:00.000"' . "\n";
        $this->assertConversion(
            $inputCsv,
            ['id', 'ts1', 'name', 'ts2'],
            [
                'ts1' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']],
                'ts2' => [['provider' => 'storage', 'key' => 'KBC.datatype.type', 'value' => 'TIMESTAMP_LTZ']],
            ],
            'America/Los_Angeles',
            '1,"2024-01-15 18:00:00.000000",hello,"2024-06-15 21:00:00.000000"' . "\n",
        );
    }

    /**
     * @param array<string, array<int, array<string, string>>> $columnMetadata
     */
    private function assertConversion(
        string $inputCsv,
        array $columns,
        array $columnMetadata,
        string $sourceTimezone,
        string $expectedCsv,
    ): void {
        $result = $this->runConversion($inputCsv, $columns, $columnMetadata, $sourceTimezone);
        $this->assertEquals($expectedCsv, $result);
    }

    /**
     * @param string[] $columns
     * @param array<string, array<int, array<string, string>>> $columnMetadata
     */
    private function runConversion(
        string $inputCsv,
        array $columns,
        array $columnMetadata,
        string $sourceTimezone,
    ): string {
        $tmpFile = tempnam(sys_get_temp_dir(), 'ts_test_');
        assert($tmpFile !== false);

        $gz = gzopen($tmpFile, 'wb');
        assert($gz !== false);
        gzwrite($gz, $inputCsv);
        gzclose($gz);

        $converter = new TimestampConverter(
            $columns,
            $columnMetadata,
            $sourceTimezone,
            new NullLogger(),
        );
        $converter->processGzippedFile($tmpFile);

        $input = fopen('compress.zlib://' . $tmpFile, 'r');
        assert($input !== false);
        $content = stream_get_contents($input);
        fclose($input);
        unlink($tmpFile);

        return (string) $content;
    }
}
