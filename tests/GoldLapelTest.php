<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class GoldLapelTest extends TestCase
{
    // -- FindBinary (3 tests) --

    private string|false $originalGoldlapelBinary;

    protected function setUp(): void
    {
        $this->originalGoldlapelBinary = getenv('GOLDLAPEL_BINARY');
    }

    protected function tearDown(): void
    {
        if ($this->originalGoldlapelBinary === false) {
            putenv('GOLDLAPEL_BINARY');
        } else {
            putenv("GOLDLAPEL_BINARY={$this->originalGoldlapelBinary}");
        }
    }

    public function testFindBinaryEnvVarOverride(): void
    {
        $tmp = tempnam(sys_get_temp_dir(), 'gl_');
        try {
            putenv("GOLDLAPEL_BINARY={$tmp}");
            $this->assertSame($tmp, GoldLapel::findBinary());
        } finally {
            unlink($tmp);
        }
    }

    public function testFindBinaryEnvVarMissingFile(): void
    {
        putenv('GOLDLAPEL_BINARY=/nonexistent/goldlapel');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/GOLDLAPEL_BINARY.*not found/');
        GoldLapel::findBinary();
    }

    public function testFindBinaryNotFoundRaises(): void
    {
        putenv('GOLDLAPEL_BINARY');
        $origPath = getenv('PATH');
        try {
            putenv('PATH=');
            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessageMatches('/Gold Lapel binary not found/');
            GoldLapel::findBinary();
        } finally {
            putenv("PATH={$origPath}");
        }
    }

    // -- MakeProxyUrl (14 tests) --

    public function testMakeProxyUrlPostgresqlFull(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:pass@dbhost:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlPostgresScheme(): void
    {
        $this->assertSame(
            'postgres://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgres://user:pass@remote.aws.com:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoPort(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:pass@host.aws.com/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoPortNoPath(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932',
            GoldLapel::makeProxyUrl('postgresql://user:pass@host.aws.com', 7932)
        );
    }

    public function testMakeProxyUrlBareHostPort(): void
    {
        $this->assertSame(
            'localhost:7932',
            GoldLapel::makeProxyUrl('dbhost:5432', 7932)
        );
    }

    public function testMakeProxyUrlBareHost(): void
    {
        $this->assertSame(
            'localhost:7932',
            GoldLapel::makeProxyUrl('dbhost', 7932)
        );
    }

    public function testMakeProxyUrlQueryParams(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb?sslmode=require',
            GoldLapel::makeProxyUrl('postgresql://user:pass@remote:5432/mydb?sslmode=require', 7932)
        );
    }

    public function testMakeProxyUrlPercentEncoded(): void
    {
        $this->assertSame(
            'postgresql://user:p%40ss@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:p%40ss@remote:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoUserWithPort(): void
    {
        $this->assertSame(
            'postgresql://localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://dbhost:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoUserNoPort(): void
    {
        $this->assertSame(
            'postgresql://localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://dbhost/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLocalhostUpstream(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:pass@localhost:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLiteralAtInPassword(): void
    {
        $this->assertSame(
            'postgresql://user:p@ss@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:p@ss@host:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLiteralAtInPasswordNoPort(): void
    {
        $this->assertSame(
            'postgresql://user:p@ss@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:p@ss@host/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLiteralAtInPasswordWithQueryParams(): void
    {
        $this->assertSame(
            'postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue',
            GoldLapel::makeProxyUrl('postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue', 7932)
        );
    }

    // -- WaitForPort (2 tests) --

    public function testWaitForPortOpenPortReturnsTrue(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);

        $name = stream_socket_get_name($server, false);
        $port = (int) explode(':', $name)[1];

        try {
            $this->assertTrue(GoldLapel::waitForPort('127.0.0.1', $port, 2.0));
        } finally {
            fclose($server);
        }
    }

    public function testWaitForPortClosedPortTimesOut(): void
    {
        $this->assertFalse(GoldLapel::waitForPort('127.0.0.1', 19999, 0.2));
    }

    // -- Construction / state (5 tests) --
    //
    // Note: we still test the direct constructor for unit-testing
    // purposes. Production code should use GoldLapel::start().

    public function testDefaultPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertSame(7932, $gl->getProxyPort());
    }

    public function testCustomPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', ['proxy_port' => 9000]);
        $this->assertSame(9000, $gl->getProxyPort());
    }

    public function testNotRunningInitially(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertFalse($gl->isRunning());
    }

    public function testStopNoOp(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $gl->stop();
        $this->assertFalse($gl->isRunning());
    }

    public function testStopIdempotent(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $gl->stop();
        $gl->stop();
        $this->assertFalse($gl->isRunning());
    }

    // -- Factory / cleanup (3 tests) --

    public function testCleanupAllNoLiveInstances(): void
    {
        // Safe to call cleanupAll() with no instances — should not throw.
        GoldLapel::cleanupAll();
        $this->assertTrue(true);
    }

    public function testUrlNullBeforeStart(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertNull($gl->url());
    }

    public function testPdoDsnNullBeforeStart(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertNull($gl->pdoDsn());
    }

    // -- configToArgs (12 tests) --

    public function testConfigStringValue(): void
    {
        $this->assertSame(
            ['--pool-mode', 'transaction'],
            GoldLapel::configToArgs(['pool_mode' => 'transaction'])
        );
    }

    public function testConfigNumericValue(): void
    {
        $this->assertSame(
            ['--min-pattern-count', '5'],
            GoldLapel::configToArgs(['min_pattern_count' => 5])
        );
    }

    public function testConfigBooleanTrue(): void
    {
        $this->assertSame(
            ['--disable-matviews'],
            GoldLapel::configToArgs(['disable_matviews' => true])
        );
    }

    public function testConfigBooleanFalse(): void
    {
        $this->assertSame(
            [],
            GoldLapel::configToArgs(['disable_matviews' => false])
        );
    }

    public function testConfigListValue(): void
    {
        $this->assertSame(
            ['--replica', 'host1:5432', '--replica', 'host2:5432'],
            GoldLapel::configToArgs(['replica' => ['host1:5432', 'host2:5432']])
        );
    }

    public function testConfigUnknownKeyThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown config key: not_a_key');
        GoldLapel::configToArgs(['not_a_key' => 'value']);
    }

    public function testConfigMultipleKeys(): void
    {
        $result = GoldLapel::configToArgs([
            'pool_mode' => 'transaction',
            'pool_size' => 10,
            'disable_rewrite' => true,
        ]);
        $this->assertSame(
            ['--pool-mode', 'transaction', '--pool-size', '10', '--disable-rewrite'],
            $result
        );
    }

    public function testConfigEmpty(): void
    {
        $this->assertSame([], GoldLapel::configToArgs([]));
    }

    public function testConfigNullValue(): void
    {
        $this->assertSame(
            ['--fallback', ''],
            GoldLapel::configToArgs(['fallback' => null])
        );
    }

    public function testConfigBooleanKeyNonBoolThrows(): void
    {
        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage("Config key 'disable_pool' must be a boolean");
        GoldLapel::configToArgs(['disable_pool' => 'yes']);
    }

    public function testConfigListKeyNonArrayThrows(): void
    {
        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage("Config key 'replica' must be an array");
        GoldLapel::configToArgs(['replica' => 'host:5432']);
    }

    public function testConfigConstructorIntegration(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', [
            'mode' => 'waiter',
            'config' => ['disable_matviews' => true],
        ]);
        $this->assertSame(7932, $gl->getProxyPort());
        $this->assertFalse($gl->isRunning());
    }

    // -- Dashboard URL (4 tests) --

    public function testDefaultDashboardPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertSame(7933, $gl->getDashboardPort());
    }

    public function testCustomDashboardPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', ['dashboard_port' => 8080]);
        $this->assertSame(8080, $gl->getDashboardPort());
    }

    public function testDashboardPortDisabledWithZero(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', ['dashboard_port' => 0]);
        $this->assertSame(0, $gl->getDashboardPort());
        $this->assertNull($gl->getDashboardUrl());
    }

    public function testDashboardUrlNullWhenNotRunning(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertNull($gl->getDashboardUrl());
    }

    public function testDashboardPortDerivesFromCustomProxyPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', ['proxy_port' => 17932]);
        $this->assertSame(17933, $gl->getDashboardPort());
    }

    public function testExplicitDashboardPortOverridesDerivation(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', ['proxy_port' => 17932, 'dashboard_port' => 9999]);
        $this->assertSame(9999, $gl->getDashboardPort());
    }

    // -- configKeys (3 tests) --

    public function testConfigKeysReturnsArray(): void
    {
        $keys = GoldLapel::configKeys();
        $this->assertIsArray($keys);
    }

    public function testConfigKeysContainsKnownKeys(): void
    {
        // Tuning knobs still live in the structured config map.
        $keys = GoldLapel::configKeys();
        $this->assertContains('pool_size', $keys);
        $this->assertContains('disable_matviews', $keys);
        $this->assertContains('replica', $keys);
    }

    public function testConfigKeysDoesNotContainPromotedTopLevelKeys(): void
    {
        // Top-level concepts (mode, log_level, dashboard_port, etc.) were
        // promoted out of the structured config map.
        $keys = GoldLapel::configKeys();
        $this->assertNotContains('mode', $keys);
        $this->assertNotContains('log_level', $keys);
        $this->assertNotContains('dashboard_port', $keys);
        $this->assertNotContains('invalidation_port', $keys);
        $this->assertNotContains('config', $keys);
        $this->assertNotContains('license', $keys);
        $this->assertNotContains('client', $keys);
    }

    public function testConfigKeysCount(): void
    {
        $keys = GoldLapel::configKeys();
        $this->assertCount(38, $keys);
    }

    // -- urlToPdoDsn (3 tests) --

    public function testUrlToPdoDsnBasic(): void
    {
        $this->assertSame(
            'pgsql:host=localhost;port=7932;dbname=mydb',
            GoldLapel::urlToPdoDsn('postgresql://user:pass@localhost:7932/mydb')
        );
    }

    public function testUrlToPdoDsnWithQueryParams(): void
    {
        $this->assertSame(
            'pgsql:host=localhost;port=7932;dbname=mydb;sslmode=require',
            GoldLapel::urlToPdoDsn('postgresql://user:pass@localhost:7932/mydb?sslmode=require')
        );
    }

    public function testUrlToPdoDsnMinimal(): void
    {
        $this->assertSame(
            'pgsql:host=localhost;port=7932',
            GoldLapel::urlToPdoDsn('postgresql://localhost:7932')
        );
    }
}
