<?php

namespace GoldLapel\Amp\Tests;

use Amp\DeferredFuture;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Amp\GoldLapel;
use GoldLapel\Tests\IntegrationGate;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

use function Amp\async;
use function Amp\Future\awaitAll;

/**
 * Regression test for a scope-leak bug class in the Amp async wrapper.
 *
 * The shape: fiber A calls `$gl->using($connA, fn () => ...)` and, while
 * the callback is suspended (awaiting some other future), fiber B is
 * scheduled and makes its own `$gl->someMethod()` call. Fiber B MUST see
 * the default connection — NOT fiber A's scoped `connA`. If it sees
 * connA, the `using()` implementation is storing scope on a shared
 * instance field and leaking across sibling fibers.
 *
 * Synchronisation is deterministic via DeferredFuture (no delay loops)
 * so the test is reliable in CI. Ruby's
 * `test_using_scope_under_async_reactor` covers the same bug class in
 * the native Ruby async wrapper — this is the PHP/Amp parity test
 * (v0.2 Tests Q7).
 */
final class ScopeLeakTest extends TestCase
{
    private static ?string $upstream = null;
    private static bool $proxyAvailable = false;

    public static function setUpBeforeClass(): void
    {
        // Evaluating the gate here surfaces the half-configured CI case
        // (GOLDLAPEL_INTEGRATION=1 set, GOLDLAPEL_TEST_UPSTREAM missing) as
        // a RuntimeException during PHPUnit setup — preventing false-green.
        self::$upstream = IntegrationGate::upstream();
        if (self::$upstream === null) {
            return;
        }

        // Upstream liveness check
        $parsed = parse_url(self::$upstream);
        if ($parsed === false || !isset($parsed['host'])) {
            return;
        }
        $fp = @fsockopen($parsed['host'], $parsed['port'] ?? 5432, $errno, $errstr, 1.0);
        if ($fp === false) {
            return;
        }
        fclose($fp);

        $envBin = getenv('GOLDLAPEL_BINARY');
        if ($envBin !== false && $envBin !== '' && is_file($envBin)) {
            self::$proxyAvailable = true;
            return;
        }
        foreach (explode(PATH_SEPARATOR, getenv('PATH') ?: '') as $p) {
            if (is_file($p . '/goldlapel') && is_executable($p . '/goldlapel')) {
                self::$proxyAvailable = true;
                return;
            }
        }
    }

    protected function setUp(): void
    {
        if (self::$upstream === null) {
            $this->markTestSkipped(IntegrationGate::skipReason());
        }
        if (!self::$proxyAvailable) {
            $this->markTestSkipped(
                'Postgres or goldlapel binary not reachable; set GOLDLAPEL_TEST_UPSTREAM + GOLDLAPEL_BINARY to enable.'
            );
        }
    }

    private function uniqPort(): int
    {
        return random_int(18000, 28000);
    }

    /**
     * Reflective accessor: returns what `resolveConn(null)` would return
     * for the fiber this is called from. Mirrors exactly what every
     * instance method's `async()` closure sees (because the fix resolves
     * in the caller's fiber before `async()` spawns the inner fiber).
     */
    private function observeScopedConn(GoldLapel $gl): PostgresExecutor
    {
        $ref = new ReflectionClass(GoldLapel::class);
        $m = $ref->getMethod('resolveConn');
        $m->setAccessible(true);
        return $m->invoke($gl, null);
    }

    public function testUsingScopeDoesNotLeakToSiblingFiber(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['port' => $port, 'silent' => true])->await();

        try {
            $defaultConn = $gl->connection();
            $this->assertInstanceOf(PostgresConnection::class, $defaultConn);

            // A distinct executor to scope in fiber A. A transaction on
            // the primary connection is a PostgresExecutor that is NOT
            // the same object as the connection itself, so the sibling-
            // fiber observation has something unambiguous to compare.
            $connA = $defaultConn->beginTransaction();
            try {
                $enterUsing = new DeferredFuture();
                $bFinished = new DeferredFuture();
                $observedByB = null;
                $observedInsideA = null;

                $fiberA = async(function () use ($gl, $connA, $enterUsing, $bFinished, &$observedInsideA) {
                    $gl->using($connA, function ($scopedGl) use ($enterUsing, $bFinished, &$observedInsideA) {
                        // Inside A's using() scope — must see connA.
                        $observedInsideA = $this->observeScopedConn($scopedGl);
                        // Release B, then park until B has observed.
                        $enterUsing->complete(true);
                        $bFinished->getFuture()->await();
                    })->await();
                });

                $fiberB = async(function () use ($gl, $enterUsing, $bFinished, &$observedByB) {
                    // Wait until A is inside using() and parked.
                    $enterUsing->getFuture()->await();
                    // Sibling fiber — MUST NOT see connA.
                    $observedByB = $this->observeScopedConn($gl);
                    $bFinished->complete(true);
                });

                awaitAll([$fiberA, $fiberB]);

                $this->assertSame(
                    $connA,
                    $observedInsideA,
                    'Precondition: fiber A inside using() should see its scoped connection',
                );
                $this->assertNotSame(
                    $connA,
                    $observedByB,
                    'Scope leak: sibling fiber B observed fiber A\'s scoped connection (using() is storing scope on shared instance state instead of fiber-local storage)',
                );
                $this->assertSame(
                    $defaultConn,
                    $observedByB,
                    'Sibling fiber B should fall back to the default connection while A is inside using()',
                );
            } finally {
                // End the transaction cleanly — rollback is fine, we
                // only used it as an executor handle.
                try {
                    $connA->rollback();
                } catch (\Throwable $e) {
                    // best-effort
                }
            }
        } finally {
            $gl->stop()->await();
        }
    }

    public function testUsingScopeDoesNotLeakOutToTheCallingFiber(): void
    {
        // After `$gl->using(...)->await()` returns, the NEXT `$gl->*`
        // call made by the caller must see the default connection, not
        // whatever was scoped inside using(). This catches a bug where
        // the `finally` restore inside using() is missing, or the scope
        // is somehow bleeding back out to the parent fiber.
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['port' => $port, 'silent' => true])->await();

        try {
            $defaultConn = $gl->connection();
            $connA = $defaultConn->beginTransaction();
            try {
                $observedInside = null;
                $observedAfter = null;

                async(function () use ($gl, $connA, &$observedInside, &$observedAfter) {
                    $gl->using($connA, function ($scopedGl) use (&$observedInside) {
                        $observedInside = $this->observeScopedConn($scopedGl);
                    })->await();
                    // After the using() block exits, the fiber's scope
                    // should be unwound — we're back to the default.
                    $observedAfter = $this->observeScopedConn($gl);
                })->await();

                $this->assertSame($connA, $observedInside, 'inside using() should see scoped conn');
                $this->assertSame(
                    $defaultConn,
                    $observedAfter,
                    'after using() exits, fiber-local scope must be cleared — next call should fall back to the default connection',
                );
            } finally {
                try { $connA->rollback(); } catch (\Throwable $e) {}
            }
        } finally {
            $gl->stop()->await();
        }
    }
}
