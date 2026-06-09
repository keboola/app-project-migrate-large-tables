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

        self::assertSame(3, $config->getGcsLargeTableParallelChunks());
        self::assertSame(150, $config->getGcsLargeTableChunkSize());
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

        self::assertSame(5, $config->getGcsLargeTableParallelChunks());
        self::assertSame(200, $config->getGcsLargeTableChunkSize());
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

    public function testForcePrimaryKeyNotNullDefaultIsFalse(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
        ]);
        self::assertFalse($config->forcePrimaryKeyNotNull());
    }

    public function testForcePrimaryKeyNotNullCanBeEnabled(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'forcePrimaryKeyNotNull' => true,
        ]);
        self::assertTrue($config->forcePrimaryKeyNotNull());
    }

    public function testReplicationStrategyDefaultsToStandalone(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
        ]);

        self::assertSame('standalone', $config->getReplicationStrategy());
        self::assertFalse($config->useReplicationGroup());
    }

    public function testReplicationGroupConfig(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
                'sourceAccountIdentifier' => 'MYORG.SOURCEACCT',
                'databases' => ['SAPI_1234', 'SAPI_5678'],
            ],
        ]);

        self::assertSame('group', $config->getReplicationStrategy());
        self::assertTrue($config->useReplicationGroup());
        self::assertSame('MIGRATE_RG_1234', $config->getReplicationGroupName());
        self::assertSame('MYORG.SOURCEACCT', $config->getReplicationGroupSourceAccountIdentifier());
        self::assertSame(['SAPI_1234', 'SAPI_5678'], $config->getReplicationGroupDatabases());
    }

    public function testReplicationGroupRequiresNameWhenStrategyGroup(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'When "replicationStrategy" is "group", "replicationGroup.name" must be set.',
        );

        $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
        ]);
    }
}
