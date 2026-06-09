<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\Configuration\ConfigDefinition;
use Keboola\Component\UserException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigTest extends TestCase
{
    protected function tearDown(): void
    {
        putenv('KBC_URL');
        parent::tearDown();
    }

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
            ],
        ]);

        self::assertSame('group', $config->getReplicationStrategy());
        self::assertTrue($config->useReplicationGroup());
        self::assertSame('MIGRATE_RG_1234', $config->getReplicationGroupName());
    }

    /**
     * @dataProvider sourceAccountIdentifierProvider
     */
    public function testSourceAccountIdentifierDerivedFromStack(string $sourceKbcUrl, string $expected): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => $sourceKbcUrl,
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
            ],
        ]);

        self::assertSame($expected, $config->getReplicationGroupSourceAccountIdentifier());
    }

    /**
     * @return array<string, array{string, string}>
     */
    public function sourceAccountIdentifierProvider(): array
    {
        return [
            'aws-us-west' => [
                'https://connection.keboola.com',
                'RL74503.KEBOOLA_AWS_US_WEST_2',
            ],
            'aws-eu-central' => [
                'https://connection.eu-central-1.keboola.com',
                'RL74503.KEBOOLA_AWS_EU_CENTRAL_1',
            ],
            'azure-north-europe' => [
                'https://connection.north-europe.azure.keboola.com',
                'RL74503.KEBOOLA_AZURE_WESTEUROPE',
            ],
            'gcp-europe-west3' => [
                'https://connection.europe-west3.gcp.keboola.com',
                'RL74503.COM_KEBOOLA_GCP_EUROPE_WEST3',
            ],
            'gcp-us-east4' => [
                'https://connection.us-east4.gcp.keboola.com',
                'RL74503.COM_KEBOOLA_GCP_US_EAST4',
            ],
        ];
    }

    public function testSourceAccountIdentifierThrowsForUnsupportedStack(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.coates.keboola.cloud',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Replication group mode is not supported for source stack '
            . '"https://connection.coates.keboola.cloud"',
        );

        $config->getReplicationGroupSourceAccountIdentifier();
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

    public function testReplicationGroupBlockedForStacksSharingDatabasePrefix(): void
    {
        // Both connection.eu-central-1 and connection.north-europe.azure use the "KEBOOLA" db prefix.
        putenv('KBC_URL=https://connection.north-europe.azure.keboola.com');
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.eu-central-1.keboola.com',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
            ],
        ]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Replication group migration is not supported between stacks that share the database '
            . 'prefix "KEBOOLA"',
        );

        $config->assertReplicationGroupStacksCompatible();
    }

    public function testReplicationGroupAllowedForStacksWithDifferentDatabasePrefix(): void
    {
        // Source connection.keboola.com uses "SAPI", target north-europe uses "KEBOOLA".
        putenv('KBC_URL=https://connection.north-europe.azure.keboola.com');
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
            ],
        ]);

        $config->assertReplicationGroupStacksCompatible();

        self::assertTrue($config->useReplicationGroup());
    }

    public function testReplicationGroupStackCheckSkippedForStandalone(): void
    {
        // Same prefix on both sides, but standalone strategy must not be blocked.
        putenv('KBC_URL=https://connection.eu-central-1.keboola.com');
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.eu-central-1.keboola.com',
            '#sourceKbcToken' => 'token',
        ]);

        $config->assertReplicationGroupStacksCompatible();

        self::assertFalse($config->useReplicationGroup());
    }

    public function testShouldDropReplicaDatabaseDefaultsTrueForStandalone(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
        ]);

        self::assertTrue($config->shouldDropReplicaDatabase());
    }

    public function testShouldDropReplicaDatabaseDefaultsFalseForGroup(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
            ],
        ]);

        self::assertFalse($config->shouldDropReplicaDatabase());
    }

    public function testShouldDropReplicaDatabaseRespectsExplicitValueInGroup(): void
    {
        $config = $this->buildConfig([
            'sourceKbcUrl' => 'https://connection.keboola.com',
            '#sourceKbcToken' => 'token',
            'replicationStrategy' => 'group',
            'replicationGroup' => [
                'name' => 'MIGRATE_RG_1234',
            ],
            'replica' => [
                'drop' => true,
            ],
        ]);

        self::assertTrue($config->shouldDropReplicaDatabase());
    }
}
