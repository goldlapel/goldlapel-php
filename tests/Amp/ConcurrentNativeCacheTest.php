<?php

namespace GoldLapel\Amp\Tests;

use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\delay;
use function Amp\Future\awaitAll;

/**
 * Cross-wrapper parity test: exercise NativeCache under concurrent
 * put / get / invalidation from many fibers.
 *
 * Context: Python, Go, and .NET ship thread-safety tests against their
 * native caches because those wrappers run in genuinely parallel threads.
 * PHP is single-process / single-request by default — BUT the Amp
 * (`GoldLapel\Amp`) path runs queries inside Revolt fibers. Fibers are
 * cooperative, not preemptive, so they only yield at explicit suspend
 * points (await / delay). The `NativeCache` methods used here (`put`,
 * `get`, `invalidateTable`, `invalidateAll`, `processSignal`, `pollSignals`)
 * contain no suspend points, so they are effectively atomic per fiber.
 *
 * The real risk is interleaving between fiber boundaries: fiber A does
 * `put`, yields, fiber B does `get` then `invalidateTable`, yields, fiber
 * A resumes and does another `put`. We force that interleaving explicitly
 * by inserting `delay(0)` between every cache op inside each fiber — the
 * EventLoop then round-robins all pending fibers. A bug where two fiber
 * boundaries corrupt the cache's internal structure (e.g. dangling
 * `tableIndex` entries, stats desync, array mutation during iteration)
 * would surface as an exception or a counter mismatch.
 *
 * Approach mirrors the Python (`TestThreadSafety`), Go, and .NET siblings:
 *   - spawn many concurrent put/get fibers
 *   - interleave invalidation signals
 *   - assert no exceptions, stats are internally consistent, no orphan
 *     entries left in the table index
 *
 * No Postgres / proxy required — operates directly on NativeCache.
 */
final class ConcurrentNativeCacheTest extends TestCase
{
    protected function setUp(): void
    {
        NativeCache::reset();
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
    }

    /**
     * Spawn many concurrent put/get fibers + a few invalidator fibers, all
     * yielding between ops so the EventLoop interleaves them. Mirrors the
     * "100 puts + 100 gets" shape from Python's TestThreadSafety.
     */
    public function testConcurrentPutGetAndInvalidate(): void
    {
        $cache = new NativeCache(maxEntries: 1000);
        $cache->setConnected(true);

        // 10 tables, 50 distinct keys per table = 500 possible entries,
        // well under the cap so LRU eviction is not the focus here.
        $tables = [];
        for ($t = 0; $t < 10; $t++) {
            $tables[] = 't' . $t;
        }

        $errors = [];

        $putter = function (int $workerId, int $ops) use ($cache, $tables, &$errors) {
            try {
                for ($i = 0; $i < $ops; $i++) {
                    $table = $tables[$i % count($tables)];
                    $sql = "SELECT * FROM {$table} WHERE id = \$1";
                    $cache->put(
                        $sql,
                        [$workerId * 1000 + $i],
                        [['id' => (string) $i]],
                        ['id'],
                    );
                    // Yield to let other fibers interleave.
                    delay(0);
                }
            } catch (\Throwable $e) {
                $errors[] = $e;
            }
        };

        $getter = function (int $workerId, int $ops) use ($cache, $tables, &$errors) {
            try {
                for ($i = 0; $i < $ops; $i++) {
                    $table = $tables[$i % count($tables)];
                    $sql = "SELECT * FROM {$table} WHERE id = \$1";
                    $cache->get($sql, [$workerId * 1000 + $i]);
                    delay(0);
                }
            } catch (\Throwable $e) {
                $errors[] = $e;
            }
        };

        $invalidator = function (int $rounds) use ($cache, $tables, &$errors) {
            try {
                for ($r = 0; $r < $rounds; $r++) {
                    // Sprinkle invalidation signals through the run. Use
                    // processSignal to exercise the same code path the
                    // push-invalidation listener does in the real Amp
                    // path (a separate fiber draining the socket).
                    $table = $tables[$r % count($tables)];
                    $cache->processSignal('I:' . $table);
                    delay(0);
                }
            } catch (\Throwable $e) {
                $errors[] = $e;
            }
        };

        // Spawn 50 putters + 50 getters + 5 invalidators, matching the
        // Python test's ~100-fibers shape.
        $futures = [];
        for ($w = 0; $w < 50; $w++) {
            $futures[] = async($putter, $w, 20);
            $futures[] = async($getter, $w, 20);
        }
        for ($w = 0; $w < 5; $w++) {
            $futures[] = async($invalidator, 10);
        }

        [$exceptions, $_values] = awaitAll($futures);

        // No fiber should have thrown.
        $this->assertSame([], $exceptions, 'fiber(s) threw: ' . implode(', ', array_map(
            fn($e) => $e::class . ': ' . $e->getMessage(),
            $exceptions,
        )));
        $this->assertSame([], $errors, 'captured errors inside fibers: ' . implode(', ', array_map(
            fn($e) => $e::class . ': ' . $e->getMessage(),
            $errors,
        )));

        // Stats invariant: every get() either hit or missed, so hits +
        // misses equals the number of get calls we made (50 workers * 20
        // ops each). If the counters desynced, a field was clobbered by
        // interleaving.
        $this->assertSame(
            50 * 20,
            $cache->statsHits + $cache->statsMisses,
            'stats hits + misses should equal total get() calls',
        );

        // Invalidations counter should be non-negative and at most the
        // number of puts — a grossly corrupted value (negative, wildly
        // larger than puts) would indicate double-counting from a race.
        $this->assertGreaterThanOrEqual(0, $cache->statsInvalidations);
        $this->assertLessThanOrEqual(
            50 * 20,
            $cache->statsInvalidations,
            'invalidations should not exceed total puts',
        );

        // Internal consistency: verify via reflection that tableIndex has
        // no dangling references to keys that no longer exist in $cache.
        // This is the most likely corruption mode if put/invalidate raced.
        $ref = new \ReflectionClass($cache);
        $cacheProp = $ref->getProperty('cache');
        $cacheProp->setAccessible(true);
        $tableIndexProp = $ref->getProperty('tableIndex');
        $tableIndexProp->setAccessible(true);
        $accessOrderProp = $ref->getProperty('accessOrder');
        $accessOrderProp->setAccessible(true);

        $liveKeys = $cacheProp->getValue($cache);
        $tableIndex = $tableIndexProp->getValue($cache);
        $accessOrder = $accessOrderProp->getValue($cache);

        foreach ($tableIndex as $table => $keys) {
            foreach (array_keys($keys) as $key) {
                $this->assertArrayHasKey(
                    $key,
                    $liveKeys,
                    "tableIndex[{$table}] references key no longer in cache",
                );
            }
        }

        // accessOrder should have exactly one entry per live cache key.
        $this->assertSame(
            array_keys($liveKeys),
            array_keys($accessOrder),
            'accessOrder and cache key sets should match',
        );
    }

    /**
     * Interleave push-invalidation signals with cache ops in the same
     * shape the real Amp wrapper uses: one fiber draining socket signals,
     * other fibers doing queries. Exercises the `processSignal` code path
     * concurrently with `put` / `get`.
     */
    public function testConcurrentSignalProcessing(): void
    {
        $cache = new NativeCache(maxEntries: 500);
        $cache->setConnected(true);

        // Seed the cache so signals have something to invalidate.
        for ($i = 0; $i < 100; $i++) {
            $table = 't' . ($i % 10);
            $cache->put(
                "SELECT * FROM {$table} WHERE id = \$1",
                [$i],
                [['id' => (string) $i]],
                ['id'],
            );
        }

        $errors = [];

        // Signal-draining fiber: mimics the push-invalidation listener.
        $signalDrainer = function () use ($cache, &$errors) {
            try {
                for ($r = 0; $r < 30; $r++) {
                    $cache->processSignal('I:t' . ($r % 10));
                    delay(0);
                }
                // Wildcard clear at the end — exercises invalidateAll
                // racing with reads.
                $cache->processSignal('I:*');
            } catch (\Throwable $e) {
                $errors[] = $e;
            }
        };

        $reader = function (int $workerId) use ($cache, &$errors) {
            try {
                for ($i = 0; $i < 100; $i++) {
                    $table = 't' . ($i % 10);
                    $cache->get("SELECT * FROM {$table} WHERE id = \$1", [$i]);
                    delay(0);
                }
            } catch (\Throwable $e) {
                $errors[] = $e;
            }
        };

        $futures = [];
        $futures[] = async($signalDrainer);
        for ($w = 0; $w < 10; $w++) {
            $futures[] = async($reader, $w);
        }

        [$exceptions, $_] = awaitAll($futures);

        $this->assertSame([], $exceptions);
        $this->assertSame([], $errors);

        // After "I:*" wildcard at the end of the signal fiber, the cache
        // must be empty and internally consistent.
        $this->assertSame(0, $cache->size());

        $ref = new \ReflectionClass($cache);
        $tableIndexProp = $ref->getProperty('tableIndex');
        $tableIndexProp->setAccessible(true);
        $accessOrderProp = $ref->getProperty('accessOrder');
        $accessOrderProp->setAccessible(true);

        $this->assertSame(
            [],
            $tableIndexProp->getValue($cache),
            'tableIndex should be empty after wildcard invalidation',
        );
        $this->assertSame(
            [],
            $accessOrderProp->getValue($cache),
            'accessOrder should be empty after wildcard invalidation',
        );
    }
}
