<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables;

use Psr\Log\LoggerInterface;

interface MigrateInterface
{
    /**
     * @return string[] List of table IDs that failed migration
     */
    public function migrate(Config $config): array;
}
