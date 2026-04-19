<?php

namespace GoldLapel\Laravel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Laravel\GoldLapelServiceProvider;
use Illuminate\Database\Connection;
use Orchestra\Testbench\TestCase;

class GoldLapelServiceProviderTest extends TestCase
{
    protected function setUp(): void
    {
        GoldLapel::reset();
        self::clearPgsqlResolver();
        parent::setUp();
    }

    protected function tearDown(): void
    {
        self::clearPgsqlResolver();
        parent::tearDown();
    }

    private static function clearPgsqlResolver(): void
    {
        $ref = new \ReflectionProperty(Connection::class, 'resolvers');
        $resolvers = $ref->getValue();
        unset($resolvers['pgsql']);
        $ref->setValue(null, $resolvers);
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

        $this->assertCount(1, GoldLapel::$calls);
        $call = GoldLapel::$calls[0];
        $this->assertSame('postgresql://admin:secret@db.example.com:5432/mydb', $call['upstream']);
        $this->assertSame(GoldLapel::DEFAULT_PORT, $call['port']);
        $this->assertSame([], $call['config']);
        $this->assertSame([], $call['extraArgs']);

        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
        $this->assertSame(GoldLapel::DEFAULT_PORT, config('database.connections.pgsql.port'));
    }

    public function testSkipsNonPgsqlConnections(): void
    {
        $this->bootProvider([
            'mysql' => ['driver' => 'mysql', 'host' => 'db.example.com'],
            'sqlite' => ['driver' => 'sqlite', 'database' => ':memory:'],
        ]);

        $this->assertCount(0, GoldLapel::$calls);
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

        $this->assertCount(0, GoldLapel::$calls);
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
                    'port' => 9000,
                    'extra_args' => ['--threshold-duration-ms', '200'],
                ],
            ],
        ]);

        $this->assertCount(1, GoldLapel::$calls);
        $call = GoldLapel::$calls[0];
        $this->assertSame(9000, $call['port']);
        $this->assertSame(['--threshold-duration-ms', '200'], $call['extraArgs']);

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
                        'mode' => 'waiter',
                        'pool_size' => 30,
                    ],
                ],
            ],
        ]);

        $this->assertCount(1, GoldLapel::$calls);
        $call = GoldLapel::$calls[0];
        $this->assertSame(['mode' => 'waiter', 'pool_size' => 30], $call['config']);
        $this->assertSame([], $call['extraArgs']);
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
                    'port' => 9000,
                    'config' => [
                        'mode' => 'waiter',
                        'disable_pool' => true,
                    ],
                    'extra_args' => ['--threshold-duration-ms', '200'],
                ],
            ],
        ]);

        $this->assertCount(1, GoldLapel::$calls);
        $call = GoldLapel::$calls[0];
        $this->assertSame(9000, $call['port']);
        $this->assertSame(['mode' => 'waiter', 'disable_pool' => true], $call['config']);
        $this->assertSame(['--threshold-duration-ms', '200'], $call['extraArgs']);

        $this->assertSame(9000, config('database.connections.pgsql.port'));
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

        $this->assertCount(1, GoldLapel::$calls);
        $call = GoldLapel::$calls[0];
        $this->assertSame([], $call['config']);
    }

    public function testMultiplePgsqlConnections(): void
    {
        $this->bootProvider([
            'primary' => [
                'driver' => 'pgsql',
                'host' => 'db1.example.com',
                'port' => '5432',
                'database' => 'app',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7932],
            ],
            'analytics' => [
                'driver' => 'pgsql',
                'host' => 'db2.example.com',
                'port' => '5432',
                'database' => 'analytics',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7933],
            ],
        ]);

        $this->assertCount(2, GoldLapel::$calls);
        $this->assertSame(7932, GoldLapel::$calls[0]['port']);
        $this->assertSame(7933, GoldLapel::$calls[1]['port']);

        $this->assertSame('127.0.0.1', config('database.connections.primary.host'));
        $this->assertSame('127.0.0.1', config('database.connections.analytics.host'));
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

        $this->assertCount(1, GoldLapel::$calls);
        $call = GoldLapel::$calls[0];
        $this->assertSame(GoldLapel::DEFAULT_PORT, $call['port']);
        $this->assertSame([], $call['config']);
        $this->assertSame([], $call['extraArgs']);
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

        $this->assertCount(1, GoldLapel::$calls);
        $this->assertSame('postgresql://urluser:urlpass@urlhost:5433/urldb', GoldLapel::$calls[0]['upstream']);
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
        $this->assertSame(GoldLapel::DEFAULT_PORT, config('database.connections.pgsql.port'));
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

        $this->assertCount(1, GoldLapel::$calls);
        $this->assertSame('postgresql://u:p@remote:5432/db', GoldLapel::$calls[0]['upstream']);
        $this->assertNull(config('database.connections.pgsql.url'));
        $this->assertSame('prefer', config('database.connections.pgsql.sslmode'));
        $this->assertSame('127.0.0.1', config('database.connections.pgsql.host'));
    }

    // ------------------------------------------------------------------
    // Octane / Swoole / RoadRunner worker lifecycle.
    //
    // Long-lived workers keep the PHP process alive across many requests;
    // relying on __destruct or the shutdown hook to terminate the proxy
    // subprocess leaks one process per worker-boot until the worker itself
    // exits. The provider registers an $app->terminating(...) callback
    // that stops each stashed GoldLapel instance; Octane invokes
    // $app->terminate() between requests, so this triggers deterministic
    // cleanup.
    // ------------------------------------------------------------------

    public function testTerminatingCallbackStopsStashedInstances(): void
    {
        $this->bootProvider([
            'primary' => [
                'driver' => 'pgsql',
                'host' => 'db1.example.com',
                'port' => '5432',
                'database' => 'app',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7940],
            ],
            'analytics' => [
                'driver' => 'pgsql',
                'host' => 'db2.example.com',
                'port' => '5432',
                'database' => 'analytics',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7941],
            ],
        ]);

        // Both instances should be alive before the worker-shutdown hook
        // fires.
        $this->assertCount(2, GoldLapel::$liveInstances, 'Both proxies should be tracked after boot.');
        foreach (GoldLapel::$liveInstances as $instance) {
            $this->assertSame(0, $instance->stopCalls);
        }

        // Simulate Octane's between-requests call. Laravel's Application
        // invokes every registered terminating callback when terminate()
        // runs, so this is the exact path Octane triggers.
        $this->app->terminate();

        // Every stashed instance must have had ->stop() called exactly once
        // and then been removed from the live-instances tracker.
        $this->assertCount(
            0,
            GoldLapel::$liveInstances,
            'All proxy instances must be stopped + released by the terminating callback.'
        );
    }

    public function testTerminatingCallbackIsIdempotent(): void
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

        $instances = array_values(GoldLapel::$liveInstances);
        $this->assertCount(1, $instances);
        $gl = $instances[0];

        // Two terminate() invocations should not double-stop the instance.
        // Laravel processes queued callbacks on each terminate(), and we
        // rely on the provider clearing $glConnections to short-circuit.
        $this->app->terminate();
        $this->app->terminate();

        $this->assertSame(
            1,
            $gl->stopCalls,
            'stop() must be called exactly once even across repeated terminate() calls.'
        );
    }

    public function testTerminatingCallbackSurvivesStopException(): void
    {
        // If one instance's stop() throws, the terminating callback must
        // still try to stop the remaining instances — worker shutdown
        // should never leak a proxy because a sibling failed.
        $provider = $this->bootProvider([
            'primary' => [
                'driver' => 'pgsql',
                'host' => 'db1.example.com',
                'port' => '5432',
                'database' => 'app',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7942],
            ],
            'analytics' => [
                'driver' => 'pgsql',
                'host' => 'db2.example.com',
                'port' => '5432',
                'database' => 'analytics',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7943],
            ],
        ]);

        $this->assertCount(2, GoldLapel::$liveInstances);

        // Swap the first connection's instance for one whose stop() throws,
        // then invoke the terminating callback and confirm the surviving
        // sibling is still stopped. We mutate $glConnections via reflection
        // since it's private provider state.
        $connRef = new \ReflectionProperty(GoldLapelServiceProvider::class, 'glConnections');
        $connRef->setAccessible(true);
        $state = $connRef->getValue($provider);
        $names = array_keys($state);

        $throwing = new class extends GoldLapel {
            public function stop(): void
            {
                parent::stop();
                throw new \RuntimeException('stop() failed');
            }
        };
        $state[$names[0]]['instance'] = $throwing;
        $connRef->setValue($provider, $state);

        // The second instance is the real spy — verify it gets stopped
        // even though the first one threw.
        $survivor = $state[$names[1]]['instance'];
        $this->assertSame(0, $survivor->stopCalls);

        $this->app->terminate();

        $this->assertSame(
            1,
            $survivor->stopCalls,
            'Surviving instance must still be stopped when a sibling stop() throws.'
        );
    }
}
