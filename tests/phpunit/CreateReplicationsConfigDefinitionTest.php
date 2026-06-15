<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\Config;
use Keboola\AppProjectMigrateLargeTables\Configuration\CreateReplicationsConfigDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class CreateReplicationsConfigDefinitionTest extends TestCase
{
    private function buildConfig(array $parameters): Config
    {
        return new Config(
            ['parameters' => $parameters],
            new CreateReplicationsConfigDefinition(),
        );
    }

    private function baseParameters(): array
    {
        return [
            'sourceKbcUrl' => 'https://connection.keboola.com',
            'sourceHost' => 'source.snowflakecomputing.com',
            'sourceUsername' => 'MIGRATE_USER',
            '#sourcePassword' => 'secret',
            'projectIdFrom' => 1234,
            'projectIdTo' => 5678,
        ];
    }

    public function testValidConfigWithPassword(): void
    {
        $config = $this->buildConfig($this->baseParameters());

        self::assertSame('standalone', $config->getParameters()['replicationStrategy']);
    }

    public function testValidConfigWithPrivateKey(): void
    {
        $parameters = $this->baseParameters();
        unset($parameters['#sourcePassword']);
        $parameters['#sourcePrivateKey'] = 'private-key';

        $config = $this->buildConfig($parameters);

        self::assertSame('private-key', $config->getParameters()['#sourcePrivateKey']);
    }

    public function testReplicationStrategyDefaultsToStandalone(): void
    {
        $config = $this->buildConfig($this->baseParameters());

        self::assertSame('standalone', $config->getParameters()['replicationStrategy']);
    }

    public function testCredentialsAreRequired(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('You must provide either privateKey or password.');

        $parameters = $this->baseParameters();
        unset($parameters['#sourcePassword']);

        $this->buildConfig($parameters);
    }

    public function testCannotProvideBothCredentials(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('You can use either privateKey or password, not both.');

        $parameters = $this->baseParameters();
        $parameters['#sourcePrivateKey'] = 'private-key';

        $this->buildConfig($parameters);
    }

    public function testGroupStrategyRequiresName(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'When "replicationStrategy" is "group", "replicationGroup.name" must be set.',
        );

        $parameters = $this->baseParameters();
        $parameters['replicationStrategy'] = 'group';
        $parameters['replicationGroup'] = [
            'databases' => ['SAPI_1234'],
        ];

        $this->buildConfig($parameters);
    }

    public function testGroupStrategyRequiresDatabases(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'When "replicationStrategy" is "group", "replicationGroup.databases" must not be empty.',
        );

        $parameters = $this->baseParameters();
        $parameters['replicationStrategy'] = 'group';
        $parameters['replicationGroup'] = [
            'name' => 'MIGRATE_RG_1234',
        ];

        $this->buildConfig($parameters);
    }

    public function testValidGroupStrategy(): void
    {
        $parameters = $this->baseParameters();
        $parameters['replicationStrategy'] = 'group';
        $parameters['replicationGroup'] = [
            'name' => 'MIGRATE_RG_1234',
            'databases' => ['SAPI_1234', 'SAPI_5678'],
        ];

        $config = $this->buildConfig($parameters);

        self::assertSame('group', $config->getParameters()['replicationStrategy']);
        self::assertSame('MIGRATE_RG_1234', $config->getParameters()['replicationGroup']['name']);
        self::assertSame(
            ['SAPI_1234', 'SAPI_5678'],
            $config->getParameters()['replicationGroup']['databases'],
        );
    }
}
