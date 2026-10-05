<?php

namespace GoldLapel\Laravel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Laravel\GoldLapelServiceProvider;
use Orchestra\Testbench\TestCase;

/**
 * The provider against the real core (allocation, reuse, option checks),
 * with FakeProxy standing in for the binary.
 */
class GoldLapelServiceProviderTest extends TestCase
{
    protected function setUp(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('The fake proxy needs python3 and a POSIX shell.');
        }
        FakeProxy::install();
        parent::setUp();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        GoldLapel::cleanupAll();
        FakeProxy::uninstall();
        unset($_SERVER['LARAVEL_OCTANE']);
    }

    /** @return list<list<string>> */
    private function spawns(): array
    {
        return FakeProxy::spawns();
    }

    /** @return array<string, GoldLapel> */
    private function glConnections(GoldLapelServiceProvider $provider): array
    {
        $ref = new \ReflectionProperty(GoldLapelServiceProvider::class, 'glConnections');
        return $ref->getValue($provider);
    }

    private function bootProvider(array $connections): GoldLapelServiceProvider
    {
        // Replace all connections to avoid Testbench defaults interfering
        config(['database.connections' => $connections]);

        $provider = new GoldLapelServiceProvider($this->app);
        $provider->boot();

        return $provider;
    }

    public function testRewritesPgsqlConnection(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'db.example.com',
                'port' => '5432',
                'database' => 'mydb',
                'username' => 'admin',
                'password' => 'secret',
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $args = $this->spawns()[0];
        $this->assertSame('postgresql://admin:secret@db.example.com:5432/mydb', FakeProxy::option($args, '--upstream'));
        // No proxy_port configured → the core allocates the first free pair.
        $this->assertSame((string) GoldLapel::DEFAULT_PROXY_PORT, FakeProxy::option($args, '--proxy-port'));

        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
        $this->assertSame(GoldLapel::DEFAULT_PROXY_PORT, config('database.connections.pgsql.port'));
    }

    public function testSkipsNonPgsqlConnections(): void
    {
        $this->bootProvider([
            'mysql' => ['driver' => 'mysql', 'host' => 'db.example.com'],
            'sqlite' => ['driver' => 'sqlite', 'database' => ':memory:'],
        ]);

        $this->assertCount(0, $this->spawns());
    }

    public function testSkipsWhenDisabled(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'db.example.com',
                'port' => '5432',
                'database' => 'mydb',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['enabled' => false],
            ],
        ]);

        $this->assertCount(0, $this->spawns());
    }

    public function testCustomPortAndExtraArgs(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => [
                    'proxy_port' => 9000,
                    'extra_args' => ['--threshold-duration-ms', '200'],
                ],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $args = $this->spawns()[0];
        $this->assertSame('9000', FakeProxy::option($args, '--proxy-port'));
        $this->assertSame('200', FakeProxy::option($args, '--threshold-duration-ms'));

        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
        $this->assertSame(9000, config('database.connections.pgsql.port'));
    }

    public function testConfigPassthrough(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => [
                    'config' => [
                        'pool_mode' => 'transaction',
                        'pool_size' => 30,
                    ],
                ],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $args = $this->spawns()[0];
        $this->assertSame('transaction', FakeProxy::option($args, '--pool-mode'));
        $this->assertSame('30', FakeProxy::option($args, '--pool-size'));
    }

    public function testConfigWithPortAndExtraArgs(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => [
                    'proxy_port' => 9000,
                    'mode' => 'waiter',
                    'config' => [
                        'disable_pool' => true,
                    ],
                    'extra_args' => ['--threshold-duration-ms', '200'],
                ],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $args = $this->spawns()[0];
        $this->assertSame('9000', FakeProxy::option($args, '--proxy-port'));
        $this->assertSame('waiter', FakeProxy::option($args, '--mode'));
        $this->assertContains('--disable-pool', $args);
        $this->assertSame('200', FakeProxy::option($args, '--threshold-duration-ms'));

        $this->assertSame(9000, config('database.connections.pgsql.port'));
    }

    public function testLogLevelForwardedToStartProxyOnly(): void
    {
        // Regression: `log_level` under the per-connection `goldlapel`
        // block was silently dropped by boot() — the provider read it into
        // a local but never passed it to startProxyOnly(). The result: a
        // `log_level: debug` setting in config/database.php had zero
        // effect on the spawned subprocess's -v/-vv/-vvv verbosity.
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => [
                    'log_level' => 'debug',
                ],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $this->assertContains('-vv', $this->spawns()[0]);
    }

    public function testLogLevelOmittedWhenNotConfigured(): void
    {
        // When log_level is not set we must NOT forward a null — that
        // would trigger the wrapper's validation ("must be a string")
        // and abort boot. Keep the options dict slim.
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $this->assertSame([], array_intersect(['-v', '-vv', '-vvv'], $this->spawns()[0]));
    }

    public function testLogLevelForwardedAlongsideOtherOptions(): void
    {
        // Verify log_level coexists cleanly with port / config / extra_args
        // — the provider builds the options dict in one pass.
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => [
                    'proxy_port' => 9001,
                    'log_level' => 'trace',
                    'mode' => 'waiter',
                    'extra_args' => ['--flag'],
                ],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $args = $this->spawns()[0];
        $this->assertSame('9001', FakeProxy::option($args, '--proxy-port'));
        $this->assertContains('-vvv', $args);
        $this->assertSame('waiter', FakeProxy::option($args, '--mode'));
        $this->assertSame('--flag', end($args));
    }

    public function testEmptyConfigArray(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => [
                    'config' => [],
                ],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
    }

    public function testMultiplePgsqlConnections(): void
    {
        // Regression: without explicit ports every connection asked for
        // 7932 (and an explicit 7933 collided with the first proxy's
        // dashboard). The provider now forwards proxy_port only when
        // configured and rewrites each connection to the port the core
        // actually allocated.
        $this->bootProvider([
            'primary' => [
                'driver' => 'pgsql',
                'host' => 'db1.example.com',
                'port' => '5432',
                'database' => 'app',
                'username' => 'u',
                'password' => 'p',
            ],
            'analytics' => [
                'driver' => 'pgsql',
                'host' => 'db2.example.com',
                'port' => '5432',
                'database' => 'analytics',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertCount(2, $this->spawns());

        $this->assertSame('127.0.0.1', config('database.connections.primary.host'));
        $this->assertSame('127.0.0.1', config('database.connections.analytics.host'));
        $this->assertSame(7932, config('database.connections.primary.port'));
        $this->assertSame(7934, config('database.connections.analytics.port'));
    }

    public function testSameUpstreamSharesOneProxy(): void
    {
        $db = [
            'driver' => 'pgsql',
            'host' => 'db1.example.com',
            'port' => '5432',
            'database' => 'app',
            'username' => 'u',
            'password' => 'p',
        ];
        $provider = $this->bootProvider(['primary' => $db, 'reporting' => $db]);

        $this->assertCount(1, $this->spawns(), 'Same upstream must reuse the running proxy.');
        $this->assertSame(7932, config('database.connections.primary.port'));
        $this->assertSame(7932, config('database.connections.reporting.port'));

        $gl = $this->glConnections($provider)['primary'];
        $this->assertSame($gl, $this->glConnections($provider)['reporting']);
        $this->app->terminate();
        $this->assertFalse($gl->isRunning(), 'Both connections release the shared proxy, which then stops.');
    }

    public function testExplicitPortAnotherConnectionsProxyHoldsIsRejected(): void
    {
        $this->bootProvider([
            'primary' => [
                'driver' => 'pgsql', 'host' => 'db1.example.com', 'port' => '5432',
                'database' => 'app', 'username' => 'u', 'password' => 'p',
            ],
            'analytics' => [
                'driver' => 'pgsql', 'host' => 'db2.example.com', 'port' => '5432',
                'database' => 'analytics', 'username' => 'u', 'password' => 'p',
                // primary's dashboard port
                'goldlapel' => ['proxy_port' => 7933],
            ],
        ]);

        $this->assertCount(1, $this->spawns(), 'The colliding proxy must not be spawned.');
        $this->assertSame(7932, config('database.connections.primary.port'));
        // Left pointing at its own database, not at primary's proxy.
        $this->assertSame('db2.example.com', config('database.connections.analytics.host'));
        $this->assertSame('5432', config('database.connections.analytics.port'));
    }

    public function testBadOptionValueIsLoggedNotFatal(): void
    {
        // configToArgs() raises \TypeError for a mistyped value; the
        // provider must log it like any other start failure.
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql', 'host' => 'h', 'port' => '5432',
                'database' => 'db', 'username' => 'u', 'password' => 'p',
                'goldlapel' => ['config' => ['disable_pool' => 'yes']],
            ],
        ]);

        $this->assertCount(0, $this->spawns());
        $this->assertSame('h', config('database.connections.pgsql.host'));
    }

    public function testUnknownOptionIsLoggedNotFatal(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql', 'host' => 'h', 'port' => '5432',
                'database' => 'db', 'username' => 'u', 'password' => 'p',
                'goldlapel' => ['invalidation_port' => 7934],
            ],
        ]);

        $this->assertCount(0, $this->spawns());
        $this->assertSame('h', config('database.connections.pgsql.host'));
    }

    public function testForwardsCoreOptions(): void
    {
        // Laravel accepts the same options as GoldLapel::start().
        $options = [
            'proxy_port' => 9000,
            'dashboard_port' => 0,
            'license' => '/etc/gl.license',
            'config_file' => '/etc/gl.toml',
            'silent' => true,
            'mesh' => true,
            'mesh_tag' => 'prod-east',
            'disable_proxy_cache' => true,
            'disable_sqloptimize' => true,
            'disable_auto_indexes' => true,
            'client' => 'my-app',
        ];
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => $options + ['enabled' => true],
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $args = $this->spawns()[0];
        $this->assertSame('9000', FakeProxy::option($args, '--proxy-port'));
        $this->assertSame('0', FakeProxy::option($args, '--dashboard-port'));
        $this->assertSame('/etc/gl.license', FakeProxy::option($args, '--license'));
        $this->assertSame('/etc/gl.toml', FakeProxy::option($args, '--config'));
        $this->assertSame('prod-east', FakeProxy::option($args, '--mesh-tag'));
        $this->assertSame('my-app', FakeProxy::option($args, '--client'));
        foreach (['--mesh', '--disable-proxy-cache', '--disable-sqloptimize', '--disable-auto-indexes'] as $flag) {
            $this->assertContains($flag, $args);
        }
    }

    public function testClientDefaultsToLaravel(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertSame('laravel', FakeProxy::option($this->spawns()[0], '--client'));
    }

    public function testDefaultsWhenNoGoldlapelConfig(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $this->assertSame(GoldLapel::DEFAULT_PROXY_PORT, config('database.connections.pgsql.port'));
    }

    public function testUrlKeyUsedForUpstream(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'url' => 'postgresql://urluser:urlpass@urlhost:5433/urldb',
                'host' => 'wrong-host',
                'port' => '9999',
                'database' => 'wrong-db',
                'username' => 'wrong-user',
                'password' => 'wrong-pass',
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $this->assertSame('postgresql://urluser:urlpass@urlhost:5433/urldb', FakeProxy::option($this->spawns()[0], '--upstream'));
    }

    public function testUrlKeyClearedAfterRewrite(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'url' => 'postgresql://u:p@remote:5432/db',
                'host' => 'remote',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertNull(config('database.connections.pgsql.url'));
        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
        $this->assertSame(GoldLapel::DEFAULT_PROXY_PORT, config('database.connections.pgsql.port'));
    }

    public function testUrlKeyClearedEvenWhenNotPresent(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertNull(config('database.connections.pgsql.url'));
    }

    public function testSslModeClearedAfterRewrite(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'remote.db.com',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
                'sslmode' => 'require',
            ],
        ]);

        $this->assertSame('prefer', config('database.connections.pgsql.sslmode'));
    }

    public function testSslModeSetToPreferWhenNotOriginallyPresent(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'h',
                'port' => '5432',
                'database' => 'db',
                'username' => 'u',
                'password' => 'p',
            ],
        ]);

        $this->assertSame('prefer', config('database.connections.pgsql.sslmode'));
    }

    public function testUrlAndSslBothHandledTogether(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'url' => 'postgresql://u:p@remote:5432/db',
                'sslmode' => 'verify-full',
            ],
        ]);

        $this->assertCount(1, $this->spawns());
        $this->assertSame('postgresql://u:p@remote:5432/db', FakeProxy::option($this->spawns()[0], '--upstream'));
        $this->assertNull(config('database.connections.pgsql.url'));
        $this->assertSame('prefer', config('database.connections.pgsql.sslmode'));
        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
    }

    // ------------------------------------------------------------------
    // Lifecycle.
    //
    // One request (PHP-FPM) or one console command boots the app once and
    // terminates it once: the provider stops its proxies on terminate.
    //
    // Octane boots the app once per worker and then runs the terminating
    // callbacks after every request (on a per-request clone of the app), so
    // there the proxies must outlive terminate and stop with the worker.
    // ------------------------------------------------------------------

    private function twoDatabases(): array
    {
        return [
            'primary' => [
                'driver' => 'pgsql', 'host' => 'db1.example.com', 'port' => '5432',
                'database' => 'app', 'username' => 'u', 'password' => 'p',
            ],
            'analytics' => [
                'driver' => 'pgsql', 'host' => 'db2.example.com', 'port' => '5432',
                'database' => 'analytics', 'username' => 'u', 'password' => 'p',
            ],
        ];
    }

    public function testTerminateStopsTheProxies(): void
    {
        $provider = $this->bootProvider($this->twoDatabases());
        $instances = $this->glConnections($provider);
        $this->assertCount(2, $instances);
        foreach ($instances as $instance) {
            $this->assertTrue($instance->isRunning());
        }

        $this->app->terminate();

        foreach ($instances as $instance) {
            $this->assertFalse($instance->isRunning());
        }
        $this->assertSame([], $this->glConnections($provider));
    }

    public function testRepeatedTerminateIsHarmless(): void
    {
        $provider = $this->bootProvider($this->twoDatabases());
        $this->app->terminate();
        $this->app->terminate();
        $this->assertSame([], $this->glConnections($provider));
    }

    public function testTerminateSurvivesAStopException(): void
    {
        // If one instance's stop() throws, the rest must still be stopped.
        $provider = $this->bootProvider($this->twoDatabases());

        $connRef = new \ReflectionProperty(GoldLapelServiceProvider::class, 'glConnections');
        $state = $connRef->getValue($provider);
        $survivor = $state['analytics'];
        $state['primary'] = new class ('postgresql://u:p@h/d') extends GoldLapel {
            public function stop(): void
            {
                throw new \RuntimeException('stop() failed');
            }
        };
        $connRef->setValue($provider, $state);

        $this->app->terminate();

        $this->assertFalse($survivor->isRunning(), 'A sibling stop() failure must not leak a proxy.');
    }

    public function testUnderOctaneProxiesOutliveRequestsAndStopWithTheWorker(): void
    {
        $_SERVER['LARAVEL_OCTANE'] = 1;
        $provider = $this->bootProvider($this->twoDatabases());
        $instances = $this->glConnections($provider);

        // Octane terminates the app after every request.
        $this->app->terminate();
        $this->app->terminate();
        foreach ($instances as $instance) {
            $this->assertTrue($instance->isRunning(), 'A request ending must not stop the worker\'s proxies.');
        }

        $this->app['events']->dispatch(\Laravel\Octane\Events\WorkerStopping::class);
        foreach ($instances as $instance) {
            $this->assertFalse($instance->isRunning());
        }
    }
}
