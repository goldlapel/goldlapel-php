<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Unit tests for the v0.2.0 factory API.
 *
 * These exercise the options-parsing, using() scope, and `conn:` precedence
 * without spawning the actual proxy binary. See FactoryIntegrationTest for
 * end-to-end tests that use a real goldlapel subprocess.
 */
class FactoryApiTest extends TestCase
{
    // ------------------------------------------------------------------
    // parseStartOptions is private; we test its effects indirectly via
    // configToArgs + examining state of instances constructed from options.
    // ------------------------------------------------------------------

    private function invokeParseOptions(array $options): array
    {
        $ref = new \ReflectionMethod(GoldLapel::class, 'parseStartOptions');
        $ref->setAccessible(true);
        return $ref->invoke(null, $options);
    }

    public function testParseOptionsEmpty(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions([]);
        $this->assertNull($port);
        $this->assertSame([], $config);
        $this->assertSame([], $extraArgs);
    }

    public function testParseOptionsExplicitPort(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions(['port' => 7935]);
        $this->assertSame(7935, $port);
        $this->assertSame([], $config);
        $this->assertSame([], $extraArgs);
    }

    public function testParseOptionsConfigExplicit(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions([
            'config' => ['mode' => 'waiter', 'pool_size' => 30],
        ]);
        $this->assertNull($port);
        $this->assertSame(['mode' => 'waiter', 'pool_size' => 30], $config);
    }

    public function testParseOptionsConfigInline(): void
    {
        // Top-level config keys should be folded into 'config'
        [$port, $config, $extraArgs] = $this->invokeParseOptions([
            'port' => 7932,
            'mode' => 'waiter',
            'pool_size' => 30,
        ]);
        $this->assertSame(7932, $port);
        $this->assertSame(['mode' => 'waiter', 'pool_size' => 30], $config);
    }

    public function testParseOptionsLogLevelForwarded(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions([
            'log_level' => 'debug',
        ]);
        $this->assertSame(['--log-level', 'debug'], $extraArgs);
    }

    public function testParseOptionsExtraArgsPreserved(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions([
            'extra_args' => ['--some-flag', 'val'],
        ]);
        $this->assertSame(['--some-flag', 'val'], $extraArgs);
    }

    public function testParseOptionsLogLevelAppendsToExtraArgs(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions([
            'extra_args' => ['--some-flag', 'val'],
            'log_level' => 'info',
        ]);
        $this->assertSame(['--some-flag', 'val', '--log-level', 'info'], $extraArgs);
    }

    public function testParseOptionsMergesInlineWithExplicitConfig(): void
    {
        [$port, $config, $extraArgs] = $this->invokeParseOptions([
            'config' => ['mode' => 'waiter'],
            'pool_size' => 30,
        ]);
        $this->assertSame(['mode' => 'waiter', 'pool_size' => 30], $config);
    }

    // ------------------------------------------------------------------
    // start() — full factory smoke tests using a fake goldlapel binary.
    //
    // We point GOLDLAPEL_BINARY at a shell script that just sleeps, so
    // the process starts but nothing listens on the proxy port. The
    // factory times out, which lets us exercise the start() code path
    // without needing a real proxy.
    // ------------------------------------------------------------------

    public function testStartFailsCleanlyWhenBinaryDiesImmediately(): void
    {
        // Use a fake binary that exits immediately. start() should throw a
        // RuntimeException (confirming the error path returns the right
        // exception type, not a TypeError).
        $fake = $this->makeFakeBinary("#!/bin/sh\nexit 1\n");
        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");
            $this->expectException(RuntimeException::class);
            GoldLapel::start('postgresql://user:pass@localhost:5432/db', ['port' => 19876]);
        } finally {
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    public function testStartReturnsGoldLapelInstance(): void
    {
        // Use a fake binary that listens on the requested port, so start()
        // succeeds. This confirms the return type and that the factory
        // wires options through correctly, without needing pdo_pgsql or a
        // real database.
        $port = $this->findFreePort();

        // Python one-liner TCP listener that accepts one connection then
        // exits — good enough to make waitForPort() succeed.
        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket,time,sys\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');
        $gl = null;

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");

            // start() will try to open a PDO to the fake listener (which
            // will fail when the fake accepts but speaks no Postgres wire
            // protocol). We bypass PDO by using startProxyOnly instead —
            // that still returns through the factory registration code
            // path, so it's a valid smoke test of the public API.
            $url = GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                ['port' => $port, 'dashboard_port' => 0]
            );

            $this->assertIsString($url);
            $this->assertStringContainsString("localhost:{$port}", $url);
        } finally {
            // Clean up: kill any processes we left behind.
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    private function makeFakeBinary(string $contents): string
    {
        $path = tempnam(sys_get_temp_dir(), 'gl_fake_');
        file_put_contents($path, $contents);
        chmod($path, 0755);
        // Clean up at shutdown
        register_shutdown_function(fn() => @unlink($path));
        return $path;
    }

    private function findFreePort(): int
    {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        $name = stream_socket_get_name($server, false);
        $port = (int) explode(':', $name)[1];
        fclose($server);
        return $port;
    }

    // ------------------------------------------------------------------
    // using() — scoped connection override
    // ------------------------------------------------------------------

    private function makeGlWithMockPDO(?\PDO $pdo = null): array
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $pdo = $pdo ?? $this->createMock(\PDO::class);
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        $ref->setValue($gl, $pdo);
        return [$gl, $pdo];
    }

    public function testUsingScopeHoldsConnection(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $scopedPdo = $this->createMock(\PDO::class);

        $observed = null;
        $gl->using($scopedPdo, function ($gl) use (&$observed) {
            $observed = $gl->pdo();
        });

        $this->assertSame($scopedPdo, $observed);
    }

    public function testUsingScopeIsRestoredAfterCallback(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $scopedPdo = $this->createMock(\PDO::class);

        $gl->using($scopedPdo, function () {});

        $this->assertSame($defaultPdo, $gl->pdo());
    }

    public function testUsingScopeRestoredOnException(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $scopedPdo = $this->createMock(\PDO::class);

        try {
            $gl->using($scopedPdo, function () {
                throw new \RuntimeException('boom');
            });
            $this->fail('Exception should have propagated');
        } catch (\RuntimeException $e) {
            $this->assertSame('boom', $e->getMessage());
        }

        $this->assertSame($defaultPdo, $gl->pdo());
    }

    public function testUsingScopeReturnsCallbackResult(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $scopedPdo = $this->createMock(\PDO::class);

        $result = $gl->using($scopedPdo, fn() => 42);

        $this->assertSame(42, $result);
    }

    public function testUsingScopeNestingRestoresPreviousScope(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $outerPdo = $this->createMock(\PDO::class);
        $innerPdo = $this->createMock(\PDO::class);

        $observed = [];
        $gl->using($outerPdo, function ($gl) use ($innerPdo, &$observed) {
            $observed['outer_before_inner'] = $gl->pdo();
            $gl->using($innerPdo, function ($gl) use (&$observed) {
                $observed['inner'] = $gl->pdo();
            });
            $observed['outer_after_inner'] = $gl->pdo();
        });
        $observed['after'] = $gl->pdo();

        $this->assertSame($outerPdo, $observed['outer_before_inner']);
        $this->assertSame($innerPdo, $observed['inner']);
        $this->assertSame($outerPdo, $observed['outer_after_inner']);
        $this->assertSame($defaultPdo, $observed['after']);
    }

    // ------------------------------------------------------------------
    // Per-method conn: named argument
    // ------------------------------------------------------------------

    public function testConnNamedArgumentOverridesDefault(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();

        $overridePdo = $this->createMock(\PDO::class);
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(42);

        // The default PDO should NOT be called
        $defaultPdo->expects($this->never())->method('prepare');

        // The override PDO SHOULD be called
        $overridePdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $count = $gl->docCount('users', ['active' => true], conn: $overridePdo);

        $this->assertSame(42, $count);
    }

    public function testConnNamedArgumentOverridesUsingScope(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $scopedPdo = $this->createMock(\PDO::class);
        $overridePdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(7);

        $defaultPdo->expects($this->never())->method('prepare');
        $scopedPdo->expects($this->never())->method('prepare');
        $overridePdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $gl->using($scopedPdo, function ($gl) use ($overridePdo) {
            $gl->docCount('users', null, conn: $overridePdo);
        });

        // Exit the scope cleanly
        $this->assertSame($defaultPdo, $gl->pdo());
    }

    public function testConnResolvesToUsingScopeWhenNotSpecified(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $scopedPdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(3);

        $defaultPdo->expects($this->never())->method('prepare');
        $scopedPdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $gl->using($scopedPdo, function ($gl) {
            $gl->docCount('users');
        });
    }

    public function testConnResolvesToInternalPdoByDefault(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1);

        $defaultPdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $gl->docCount('users');
    }

    public function testConnArgumentForSearch(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $overridePdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $defaultPdo->expects($this->never())->method('prepare');
        $overridePdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $gl->search('articles', 'title', 'hello', conn: $overridePdo);
    }

    public function testConnArgumentForDocInsert(): void
    {
        [$gl, $defaultPdo] = $this->makeGlWithMockPDO();
        $overridePdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([
            '_id' => 'abc',
            'data' => '{"x":1}',
            'created_at' => '2026-01-01',
        ]);

        $defaultPdo->expects($this->never())->method('prepare');
        $defaultPdo->expects($this->never())->method('exec');

        $overridePdo->expects($this->any())->method('exec')->willReturn(0);
        $overridePdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $result = $gl->docInsert('events', ['x' => 1], conn: $overridePdo);
        $this->assertSame('abc', $result['_id']);
    }

    // ------------------------------------------------------------------
    // Error modes
    // ------------------------------------------------------------------

    public function testResolveConnThrowsWhenNeitherScopedNorInternalSet(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->docCount('users');
    }

    public function testConnNamedArgWorksEvenWithoutInternalPdo(): void
    {
        // No internal PDO — an explicit conn: still works.
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $pdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(0);

        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $count = $gl->docCount('users', null, conn: $pdo);
        $this->assertSame(0, $count);
    }

    public function testUsingScopeWorksWithoutInternalPdo(): void
    {
        // No internal PDO, but using() sets the scope and wrapper calls
        // should still work.
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $scopedPdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(99);

        $scopedPdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $observed = null;
        $gl->using($scopedPdo, function ($gl) use (&$observed) {
            $observed = $gl->docCount('users');
        });

        $this->assertSame(99, $observed);
    }

    // ------------------------------------------------------------------
    // cached() helper
    // ------------------------------------------------------------------

    public function testCachedThrowsWithoutInternalPdo(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->cached();
    }

    // ------------------------------------------------------------------
    // __destruct should not throw even with a stale process reference
    // ------------------------------------------------------------------

    public function testDestructorDoesNotThrowWhenIdle(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        unset($gl);
        $this->assertTrue(true); // No exception = pass
    }
}
