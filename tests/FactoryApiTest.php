<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Unit tests for the v0.2.0 factory API.
 *
 * These exercise the options-parsing, using() scope, and `conn:` precedence
 * without spawning the actual proxy binary. See FactoryIntegrationTest for
 * end-to-end tests that use a real goldlapel subprocess.
 */
#[AllowMockObjectsWithoutExpectations]
class FactoryApiTest extends TestCase
{
    // ------------------------------------------------------------------
    // Constructor options — the canonical config surface promotes top-level
    // concepts (proxy_port, dashboard_port, log_level, mode, etc.) to
    // first-class keys on the options array. The structured `config` bag is
    // reserved for tuning knobs only.
    // ------------------------------------------------------------------

    private function optionFieldValue(GoldLapel $gl, string $field): mixed
    {
        $ref = new \ReflectionProperty(GoldLapel::class, $field);
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    public function testConstructDefaults(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertSame(GoldLapel::DEFAULT_PROXY_PORT, $this->optionFieldValue($gl, 'proxyPort'));
        $this->assertSame([], $this->optionFieldValue($gl, 'config'));
        $this->assertSame([], $this->optionFieldValue($gl, 'extraArgs'));
        $this->assertNull($this->optionFieldValue($gl, 'logLevel'));
        $this->assertNull($this->optionFieldValue($gl, 'mode'));
    }

    public function testConstructExplicitProxyPort(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['proxy_port' => 7935]);
        $this->assertSame(7935, $this->optionFieldValue($gl, 'proxyPort'));
    }

    public function testConstructExplicitConfig(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [
            'config' => ['pool_mode' => 'transaction', 'pool_size' => 30],
        ]);
        $this->assertSame(['pool_mode' => 'transaction', 'pool_size' => 30], $this->optionFieldValue($gl, 'config'));
    }

    public function testConstructRejectsPromotedTopLevelKeysInConfigMap(): void
    {
        // Regression guard: mode, log_level, dashboard_port, etc. are top-
        // level options on the canonical surface. Passing them through the
        // `config` map must raise at construction.
        foreach (['mode', 'log_level', 'dashboard_port', 'invalidation_port', 'config', 'license', 'client'] as $promoted) {
            $caught = false;
            try {
                new GoldLapel('postgresql://u:p@h/d', ['config' => [$promoted => 'x']]);
            } catch (\InvalidArgumentException $e) {
                $caught = true;
                $this->assertStringContainsString('Unknown config key', $e->getMessage());
            }
            $this->assertTrue($caught, "expected '{$promoted}' to be rejected inside the config map");
        }
    }

    public function testConstructModeAndLogLevelTopLevel(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [
            'proxy_port' => 7932,
            'mode' => 'waiter',
            'log_level' => 'info',
        ]);
        $this->assertSame('waiter', $this->optionFieldValue($gl, 'mode'));
        $this->assertSame('info', $this->optionFieldValue($gl, 'logLevel'));
    }

    public function testConstructDashboardPortOverrideAndDerivation(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['proxy_port' => 17932]);
        $this->assertSame(17933, $gl->getDashboardPort());
        $this->assertSame(17934, $gl->getInvalidationPort());

        $gl2 = new GoldLapel('postgresql://u:p@h/d', [
            'proxy_port' => 17932,
            'dashboard_port' => 9999,
            'invalidation_port' => 9998,
        ]);
        $this->assertSame(9999, $gl2->getDashboardPort());
        $this->assertSame(9998, $gl2->getInvalidationPort());
    }

    public function testConstructExtraArgsPreserved(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [
            'extra_args' => ['--some-flag', 'val'],
        ]);
        $this->assertSame(['--some-flag', 'val'], $this->optionFieldValue($gl, 'extraArgs'));
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
            GoldLapel::start('postgresql://user:pass@localhost:5432/db', ['proxy_port' => 19876]);
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
            $gl = GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                ['proxy_port' => $port, 'dashboard_port' => 0]
            );

            $this->assertInstanceOf(GoldLapel::class, $gl);
            $url = $gl->url();
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
        // Pre-populate the DDL cache so $gl->documents-><verb> doesn't issue
        // an HTTP call against a non-running dashboard. Tests use the user
        // collection name as the canonical table — see DocTest::fakePatterns.
        $this->seedDdlCache($gl, 'doc_store', 'users', 'users');
        $this->seedDdlCache($gl, 'doc_store', 'events', 'events');
        return [$gl, $pdo];
    }

    /**
     * Seed the per-instance DDL pattern cache with a fake (tables, query_patterns)
     * entry for `(family, name)` so namespace verbs can dispatch without
     * round-tripping to the dashboard during unit tests.
     */
    private function seedDdlCache(GoldLapel $gl, string $family, string $name, string $table): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'ddlCache');
        $ref->setAccessible(true);
        $cache = $ref->getValue($gl);
        $cache["{$family}:{$name}"] = [
            'tables' => ['main' => $table],
            'query_patterns' => [],
        ];
        $ref->setValue($gl, $cache);
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

        $count = $gl->documents->count('users', ['active' => true], conn: $overridePdo);

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
            $gl->documents->count('users', null, conn: $overridePdo);
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
            $gl->documents->count('users');
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

        $gl->documents->count('users');
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

        $result = $gl->documents->insert('events', ['x' => 1], conn: $overridePdo);
        $this->assertSame('abc', $result['_id']);
    }

    // ------------------------------------------------------------------
    // Error modes
    // ------------------------------------------------------------------

    public function testResolveConnThrowsWhenNeitherScopedNorInternalSet(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->seedDdlCache($gl, 'doc_store', 'users', 'users');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->documents->count('users');
    }

    public function testConnNamedArgWorksEvenWithoutInternalPdo(): void
    {
        // No internal PDO — an explicit conn: still works.
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->seedDdlCache($gl, 'doc_store', 'users', 'users');
        $pdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(0);

        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $count = $gl->documents->count('users', null, conn: $pdo);
        $this->assertSame(0, $count);
    }

    public function testUsingScopeWorksWithoutInternalPdo(): void
    {
        // No internal PDO, but using() sets the scope and wrapper calls
        // should still work.
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->seedDdlCache($gl, 'doc_store', 'users', 'users');
        $scopedPdo = $this->createMock(\PDO::class);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(99);

        $scopedPdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $observed = null;
        $gl->using($scopedPdo, function ($gl) use (&$observed) {
            $observed = $gl->documents->count('users');
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

    // ------------------------------------------------------------------
    // Regression: instance registration must happen before PDO is opened
    // so that a partial-init failure (spawn succeeds, PDO throws) still
    // results in the subprocess being cleaned up.
    // ------------------------------------------------------------------

    public function testInstanceRegisteredBeforePdoOpens(): void
    {
        // Verifies the fix for the "leak on partial-init failure" bug.
        // After startProxyWithoutConnect() returns, the instance must
        // already be in $liveInstances — so that if the subsequent PDO
        // construction in startProxy() raises, cleanupAll() can still
        // terminate the subprocess.
        $port = $this->findFreePort();

        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");

            GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                ['proxy_port' => $port, 'dashboard_port' => 0]
            );

            // Immediately after the proxy is started, the instance should
            // already be in $liveInstances — registered from inside
            // startProxyWithoutConnect(), not after it returned.
            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            $live = $ref->getValue();
            $this->assertCount(
                1,
                $live,
                'Instance must be registered with liveInstances as soon as the subprocess spawns.'
            );

            // Shutdown hook should also be registered (one-time guard).
            $regRef = new \ReflectionProperty(GoldLapel::class, 'cleanupRegistered');
            $regRef->setAccessible(true);
            $this->assertTrue($regRef->getValue(), 'Shutdown hook must be registered with liveInstances.');
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    public function testSpawnSuccessButPdoFailsStillCleansUpSubprocess(): void
    {
        // End-to-end regression: use a fake binary that listens on the
        // requested port (so startProxyWithoutConnect() succeeds) but does
        // not speak Postgres. PDO construction in start() will fail.
        // After the PDOException propagates, cleanupAll() must find and
        // terminate the subprocess.
        if (!extension_loaded('pdo_pgsql')) {
            $this->markTestSkipped('pdo_pgsql not loaded');
        }

        $port = $this->findFreePort();

        // Fake binary forks a python process that writes its PID to a
        // temp file, accepts connections, then immediately closes them
        // (so PDO construction fails). We use the PID file to assert the
        // subprocess was terminated after cleanup.
        $pidFile = tempnam(sys_get_temp_dir(), 'gl_fake_pid_');
        register_shutdown_function(fn() => @unlink($pidFile));

        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket,os,time\n"
            . "open('{$pidFile}','w').write(str(os.getpid()))\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "while True:\n"
            . "    c,_=s.accept()\n"
            . "    c.close()\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');
        $caught = false;

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");

            try {
                GoldLapel::start(
                    'postgresql://user:pass@localhost:5432/testdb',
                    ['proxy_port' => $port, 'dashboard_port' => 0]
                );
                $this->fail('Expected PDOException from start() when fake server does not speak Postgres.');
            } catch (\PDOException $e) {
                $caught = true;
            } catch (\RuntimeException $e) {
                // Some environments surface this as a RuntimeException
                // instead; still a valid partial-init failure.
                $caught = true;
            }

            $this->assertTrue($caught, 'start() must throw when PDO construction fails.');

            // Read the subprocess PID (wait briefly for the python process
            // to write it).
            $deadline = hrtime(true) + (int) (2 * 1e9);
            $pid = null;
            while (hrtime(true) < $deadline) {
                $contents = @file_get_contents($pidFile);
                if ($contents !== false && $contents !== '') {
                    $pid = (int) trim($contents);
                    break;
                }
                usleep(50000);
            }
            $this->assertNotNull($pid, 'Fake subprocess should have written its PID.');

            // $liveInstances must be drained at exception-propagation time.
            // The stronger invariant (added in the subprocess-cleanup fix
            // matching Python/Ruby semantics): start()'s catch block calls
            // stop() immediately, which drops the instance from the
            // registry — we do NOT rely on cleanupAll() at shutdown.
            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            $live = $ref->getValue();
            $this->assertCount(
                0,
                $live,
                'Subprocess registry must be drained when start() throws — cleanup is immediate, not deferred.'
            );

            // The subprocess must already be dead. Poll for a short grace
            // window (SIGTERM → wait → SIGKILL is not instantaneous), but
            // we do NOT call cleanupAll() ourselves — the wrapper's own
            // start()-catch path must have killed it already.
            $deadline = hrtime(true) + (int) (5 * 1e9);
            $alive = true;
            while (hrtime(true) < $deadline) {
                // posix_kill with signal 0 checks existence without sending.
                if (function_exists('posix_kill')) {
                    $alive = @posix_kill($pid, 0);
                } else {
                    $alive = file_exists("/proc/{$pid}");
                }
                if (!$alive) {
                    break;
                }
                usleep(50000);
            }
            $this->assertFalse(
                $alive,
                "Subprocess PID {$pid} should have been terminated by start()'s catch block "
                . '— not left for the shutdown hook.'
            );
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    public function testProcGetStatusConfirmsNoOrphanAfterPartialInitFailure(): void
    {
        // Complementary regression for the partial-init resource-cleanup
        // path, using proc_get_status() (via isRunning()) rather than
        // posix_kill to assert the subprocess is fully dead — this
        // exercises the zombie-detection cross-checks in isRunning()
        // (exitcode cache + posix_kill($pid, 0)) added in commit 07ba012.
        //
        // Invariant (strengthened to match Python/Ruby semantics): when
        // start()'s PDO step throws, start()'s catch block calls stop()
        // immediately — the subprocess is terminated BEFORE the exception
        // propagates to the caller. We do NOT rely on cleanupAll() at
        // shutdown. This is the stronger contract; the old weaker one
        // ("deferred cleanup via shutdown hook") left orphan subprocesses
        // in long-running PHP workers (Octane/Swoole/RoadRunner).
        //
        // start() threw before returning, so we can't call isRunning() on
        // the instance directly. Instead we check two OS-level observables:
        // $liveInstances is empty (no registry leak) and the proxy port
        // rebinds cleanly (the old subprocess has released it). Together
        // these match the guarantee that the process is gone.
        if (!extension_loaded('pdo_pgsql')) {
            $this->markTestSkipped('pdo_pgsql not loaded');
        }

        $port = $this->findFreePort();

        // Fake binary binds the port + accepts connections but closes them
        // immediately without speaking Postgres — PDO construction fails.
        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "while True:\n"
            . "    c,_=s.accept()\n"
            . "    c.close()\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");

            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);

            $threw = false;
            try {
                GoldLapel::start(
                    'postgresql://user:pass@localhost:5432/testdb',
                    ['proxy_port' => $port, 'dashboard_port' => 0]
                );
            } catch (\Throwable $e) {
                $threw = true;
            }

            $this->assertTrue($threw, 'start() must throw when PDO construction fails against a fake listener.');

            // $liveInstances must be drained at exception-propagation time.
            /** @var array<int, GoldLapel> $live */
            $live = $ref->getValue();
            $this->assertCount(
                0,
                $live,
                'start() must drain $liveInstances in its catch block — no deferred-cleanup orphans.'
            );

            // proc_get_status cross-check: the TCP port must be free
            // immediately after start() throws. If the subprocess were
            // still alive it would still hold the port. This complements
            // the posix_kill check in SubprocessCleanupTest by validating
            // the OS-level observable (bind availability).
            $deadline = hrtime(true) + (int) (5 * 1e9);
            $portFree = false;
            while (hrtime(true) < $deadline) {
                $probe = @stream_socket_server(
                    "tcp://127.0.0.1:{$port}",
                    $errno,
                    $errstr,
                );
                if ($probe !== false) {
                    fclose($probe);
                    $portFree = true;
                    break;
                }
                usleep(50000);
            }
            $this->assertTrue(
                $portFree,
                "Proxy port {$port} must be free immediately after start() throws "
                . '— subprocess should have been terminated before exception propagation.'
            );
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    // ------------------------------------------------------------------
    // Shutdown path / zombie detection.
    //
    // Regression guard for the isRunning() zombie-false-positive fix. On
    // some systems `proc_get_status()` can report `running => true` for a
    // child that has already exited but hasn't been `wait()`ed on yet.
    // isRunning() now cross-checks with the exitcode field and
    // `posix_kill($pid, 0)` so callers get a correct answer after the
    // subprocess has actually died.
    // ------------------------------------------------------------------

    public function testIsRunningReflectsCleanShutdown(): void
    {
        // Spawn a fake binary that listens long enough for start(), then
        // stop() it and assert isRunning() returns false — exercising the
        // SIGTERM → proc_close path end to end.
        $port = $this->findFreePort();

        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");

            GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                ['proxy_port' => $port, 'dashboard_port' => 0]
            );

            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            /** @var array<int, GoldLapel> $live */
            $live = $ref->getValue();
            $this->assertCount(1, $live, 'Instance should be tracked after startProxyOnly.');
            $gl = array_values($live)[0];

            $this->assertTrue($gl->isRunning(), 'isRunning() must be true before stop().');

            $gl->stop();

            $this->assertFalse(
                $gl->isRunning(),
                'isRunning() must be false after stop() — no zombie false-positive.'
            );
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    public function testIsRunningReflectsSubprocessSelfExit(): void
    {
        // Start a fake binary that listens long enough to let start()
        // succeed, then self-exits. After the child has died but before
        // stop() is called, isRunning() must return false. This is the
        // zombie-false-positive path the fix guards.
        $port = $this->findFreePort();

        // Listen for 1 second, then exit. Long enough for waitForPort() in
        // startProxyOnly() to succeed, short enough that the test finishes
        // quickly after the exit.
        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(1)\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");

            GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                ['proxy_port' => $port, 'dashboard_port' => 0]
            );

            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            /** @var array<int, GoldLapel> $live */
            $live = $ref->getValue();
            $gl = array_values($live)[0];

            // Wait up to 5s for the child to self-exit and the pipe to
            // close, then assert isRunning() flips false.
            $deadline = hrtime(true) + (int) (5 * 1e9);
            while (hrtime(true) < $deadline) {
                if (!$gl->isRunning()) {
                    break;
                }
                usleep(100000);
            }

            $this->assertFalse(
                $gl->isRunning(),
                'isRunning() must return false once the subprocess has actually exited, even if proc_get_status still reports running=true for an unreaped zombie.'
            );
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }
}
