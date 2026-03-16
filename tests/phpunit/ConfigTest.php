<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\Configuration\ConfigDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends TestCase
{
    private function buildConfig(array $parameters): Config
    {
        return new Config(
            ['parameters' => $parameters],
            new ConfigDefinition(),
        );
    }

    public function testGcsLargeTableDefaults(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
        ]);

        $this->assertSame(3, $config->getGcsLargeTableParallelChunks());
        $this->assertSame(150, $config->getGcsLargeTableChunkSize());
    }

    public function testGcsLargeTableCustomValues(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'gcsLargeTable' => [
                'parallelChunks' => 5,
                'chunkSize' => 200,
            ],
        ]);

        $this->assertSame(5, $config->getGcsLargeTableParallelChunks());
        $this->assertSame(200, $config->getGcsLargeTableChunkSize());
    }

    public function testParallelChunksMinimumIsOne(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value 0 is too small for path "root.parameters.gcsLargeTable.parallelChunks". ' .
            'Should be greater than or equal to 1',
        );

        $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'gcsLargeTable' => [
                'parallelChunks' => 0,
            ],
        ]);
    }

    public function testParallelChunksMaximumIsTwenty(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value 21 is too big for path "root.parameters.gcsLargeTable.parallelChunks". ' .
            'Should be less than or equal to 20',
        );

        $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'gcsLargeTable' => [
                'parallelChunks' => 21,
            ],
        ]);
    }

    public function testChunkSizeMinimumIsOne(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value 0 is too small for path "root.parameters.gcsLargeTable.chunkSize". ' .
            'Should be greater than or equal to 1',
        );

        $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'gcsLargeTable' => [
                'chunkSize' => 0,
            ],
        ]);
    }
}
