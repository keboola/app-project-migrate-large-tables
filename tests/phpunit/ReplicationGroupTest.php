<?php

declare(strict_types=1);

namespace Keboola\AppProjectMigrateLargeTables\Tests;

use Keboola\AppProjectMigrateLargeTables\Strategy\ReplicationGroup;
use PHPUnit\Framework\TestCase;

class ReplicationGroupTest extends TestCase
{
    public function testCreatePrimarySql(): void
    {
        $group = new ReplicationGroup('MIGRATE_RG_1234', ['SAPI_1234', 'SAPI_5678']);

        $sql = $group->createPrimarySql('MYORG.TARGETACCT');

        self::assertSame(
            'CREATE REPLICATION GROUP "MIGRATE_RG_1234" '
            . 'OBJECT_TYPES = DATABASES '
            . 'ALLOWED_DATABASES = "SAPI_1234", "SAPI_5678" '
            . 'ALLOWED_ACCOUNTS = MYORG.TARGETACCT;',
            $sql,
        );
    }

    public function testCreateSecondarySql(): void
    {
        $group = new ReplicationGroup('MIGRATE_RG_1234', ['SAPI_1234']);

        $sql = $group->createSecondarySql('SRCORG.SRCACCT');

        self::assertSame(
            'CREATE REPLICATION GROUP "MIGRATE_RG_1234" '
            . 'AS REPLICA OF SRCORG.SRCACCT."MIGRATE_RG_1234";',
            $sql,
        );
    }

    public function testRefreshSql(): void
    {
        $group = new ReplicationGroup('MIGRATE_RG_1234', ['SAPI_1234']);

        self::assertSame(
            'ALTER REPLICATION GROUP "MIGRATE_RG_1234" REFRESH;',
            $group->refreshSql(),
        );
    }

    public function testDropSql(): void
    {
        $group = new ReplicationGroup('MIGRATE_RG_1234', ['SAPI_1234']);

        self::assertSame(
            'DROP REPLICATION GROUP IF EXISTS "MIGRATE_RG_1234";',
            $group->dropSql(),
        );
    }

    public function testGetDatabases(): void
    {
        $group = new ReplicationGroup('RG', ['A', 'B']);
        self::assertSame(['A', 'B'], $group->getDatabases());
    }

    public function testAlterAllowedDatabasesSql(): void
    {
        $group = new ReplicationGroup('RG', ['A', 'B']);
        self::assertSame(
            'ALTER REPLICATION GROUP "RG" SET ALLOWED_DATABASES = "A", "B";',
            $group->alterAllowedDatabasesSql(),
        );
    }

    public function testAlterAllowedAccountsSql(): void
    {
        $group = new ReplicationGroup('RG', ['A']);
        self::assertSame(
            'ALTER REPLICATION GROUP "RG" SET ALLOWED_ACCOUNTS = ORG.ACCT;',
            $group->alterAllowedAccountsSql('ORG.ACCT'),
        );
    }
}
