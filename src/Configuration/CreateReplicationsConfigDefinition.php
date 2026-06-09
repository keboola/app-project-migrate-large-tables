<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class CreateReplicationsConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->scalarNode('sourceKbcUrl')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('sourceHost')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('sourceUsername')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('#sourcePassword')->cannotBeEmpty()->end()
                ->scalarNode('#sourcePrivateKey')->cannotBeEmpty()->end()
                ->integerNode('projectIdFrom')->isRequired()->end()
                ->integerNode('projectIdTo')->isRequired()->end()
                ->enumNode('replicationStrategy')
                    ->values(['standalone', 'group'])
                    ->defaultValue('standalone')
                ->end()
                ->arrayNode('replicationGroup')
                    ->children()
                        ->scalarNode('name')->cannotBeEmpty()->end()
                        ->arrayNode('databases')->prototype('scalar')->end()->end()
                    ->end()
                ->end()
            ->end()
            ->validate()->always(function ($v) {
                if (!empty($v['#privateKey']) && !empty($v['#password'])) {
                    throw new InvalidConfigurationException(
                        'You can use either privateKey or password, not both.',
                    );
                }
                if (empty($v['#privateKey']) && empty($v['#password'])) {
                    throw new InvalidConfigurationException(
                        'You must provide either privateKey or password.',
                    );
                }
                if (($v['replicationStrategy'] ?? 'standalone') === 'group') {
                    if (empty($v['replicationGroup']['name'])) {
                        throw new InvalidConfigurationException(
                            'When "replicationStrategy" is "group", "replicationGroup.name" must be set.',
                        );
                    }
                    if (empty($v['replicationGroup']['databases'])) {
                        throw new InvalidConfigurationException(
                            'When "replicationStrategy" is "group", ' .
                            '"replicationGroup.databases" must not be empty.',
                        );
                    }
                }
                return $v;
            })->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
