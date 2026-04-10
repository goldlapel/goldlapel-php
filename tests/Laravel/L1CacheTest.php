<?php

namespace GoldLapel\Laravel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Laravel\GoldLapelConnection;
use GoldLapel\Laravel\GoldLapelServiceProvider;
use Illuminate\Database\Connection;
use Illuminate\Database\PostgresConnection;
use Orchestra\Testbench\TestCase;

class L1CacheTest extends TestCase
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

    private function bootProvider(array $connections): void
    {
        config(['database.connections' => $connections]);

        $provider = new GoldLapelServiceProvider($this->app);
        $provider->boot();
    }

    public function testResolverRegisteredForGlConnection(): void
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

        $resolver = Connection::getResolver('pgsql');
        $this->assertNotNull($resolver, 'pgsql resolver should be registered');
    }

    public function testResolverNotRegisteredWhenNoGlConnections(): void
    {
        $this->bootProvider([
            'mysql' => ['driver' => 'mysql', 'host' => 'h'],
        ]);

        $resolver = Connection::getResolver('pgsql');
        $this->assertNull($resolver, 'pgsql resolver should not be registered when no GL connections');
    }

    public function testResolverReturnsGoldLapelConnectionForGlConnection(): void
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

        $resolver = Connection::getResolver('pgsql');
        $pdo = fn () => new \PDO('sqlite::memory:');
        $conn = $resolver($pdo, 'mydb', '', ['name' => 'pgsql', 'driver' => 'pgsql']);

        $this->assertInstanceOf(GoldLapelConnection::class, $conn);
    }

    public function testResolverReturnsPgsqlConnectionForNonGlConnection(): void
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

        $resolver = Connection::getResolver('pgsql');
        $pdo = fn () => new \PDO('sqlite::memory:');
        $conn = $resolver($pdo, 'other_db', '', ['name' => 'other_conn', 'driver' => 'pgsql']);

        $this->assertInstanceOf(PostgresConnection::class, $conn);
        $this->assertNotInstanceOf(GoldLapelConnection::class, $conn);
    }

    private function makeFakePdo(): \PDO
    {
        return $this->createStub(\PDO::class);
    }

    public function testWrapPDOCalledOnGetCachedPDO(): void
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

        $resolver = Connection::getResolver('pgsql');
        $fakePdo = $this->makeFakePdo();
        $conn = $resolver(fn () => $fakePdo, 'mydb', '', ['name' => 'pgsql', 'driver' => 'pgsql']);

        $this->assertCount(0, GoldLapel::$wrapCalls);

        $cached = $conn->getCachedPDO();

        $this->assertCount(1, GoldLapel::$wrapCalls);
        $this->assertSame($fakePdo, GoldLapel::$wrapCalls[0]['pdo']);
        $this->assertNull(GoldLapel::$wrapCalls[0]['invalidationPort']);
    }

    public function testCustomInvalidationPortPassedToWrapPDO(): void
    {
        $this->bootProvider([
            'pgsql' => [
                'driver' => 'pgsql',
                'host' => 'db.example.com',
                'port' => '5432',
                'database' => 'mydb',
                'username' => 'admin',
                'password' => 'secret',
                'goldlapel' => [
                    'invalidation_port' => 9500,
                ],
            ],
        ]);

        $resolver = Connection::getResolver('pgsql');
        $fakePdo = $this->makeFakePdo();
        $conn = $resolver(fn () => $fakePdo, 'mydb', '', ['name' => 'pgsql', 'driver' => 'pgsql']);

        $conn->getCachedPDO();

        $this->assertCount(1, GoldLapel::$wrapCalls);
        $this->assertSame(9500, GoldLapel::$wrapCalls[0]['invalidationPort']);
    }

    public function testCachedPDOIsLazy(): void
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

        $resolver = Connection::getResolver('pgsql');
        $conn = $resolver(fn () => $this->makeFakePdo(), 'mydb', '', ['name' => 'pgsql', 'driver' => 'pgsql']);

        // CachedPDO should not be created yet
        $this->assertCount(0, GoldLapel::$wrapCalls);

        // First call creates it
        $conn->getCachedPDO();
        $this->assertCount(1, GoldLapel::$wrapCalls);

        // Second call returns same instance (no new wrap call)
        $conn->getCachedPDO();
        $this->assertCount(1, GoldLapel::$wrapCalls);
    }

    public function testMultipleConnectionsWithDifferentInvalidationPorts(): void
    {
        $this->bootProvider([
            'primary' => [
                'driver' => 'pgsql',
                'host' => 'db1.example.com',
                'port' => '5432',
                'database' => 'app',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7932, 'invalidation_port' => 9500],
            ],
            'analytics' => [
                'driver' => 'pgsql',
                'host' => 'db2.example.com',
                'port' => '5432',
                'database' => 'analytics',
                'username' => 'u',
                'password' => 'p',
                'goldlapel' => ['port' => 7933, 'invalidation_port' => 9501],
            ],
        ]);

        $resolver = Connection::getResolver('pgsql');

        $conn1 = $resolver(fn () => $this->makeFakePdo(), 'app', '', ['name' => 'primary', 'driver' => 'pgsql']);
        $conn1->getCachedPDO();
        $this->assertSame(9500, GoldLapel::$wrapCalls[0]['invalidationPort']);

        $conn2 = $resolver(fn () => $this->makeFakePdo(), 'analytics', '', ['name' => 'analytics', 'driver' => 'pgsql']);
        $conn2->getCachedPDO();
        $this->assertSame(9501, GoldLapel::$wrapCalls[1]['invalidationPort']);
    }

    public function testDisabledConnectionDoesNotGetGoldLapelConnection(): void
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

        // No GL connections, so no resolver registered
        $resolver = Connection::getResolver('pgsql');
        $this->assertNull($resolver);
    }

    public function testDisconnectClearsCachedPDO(): void
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

        $resolver = Connection::getResolver('pgsql');
        $conn = $resolver(fn () => $this->makeFakePdo(), 'mydb', '', ['name' => 'pgsql', 'driver' => 'pgsql']);

        $conn->getCachedPDO();
        $this->assertCount(1, GoldLapel::$wrapCalls);

        $conn->disconnect();

        // Reconnect with a new PDO — wrapPDO should be called again
        $conn->setPdo(fn () => $this->makeFakePdo());
        $conn->getCachedPDO();
        $this->assertCount(2, GoldLapel::$wrapCalls);
    }
}
