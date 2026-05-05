<?php

namespace GoldLapel\Amp\Tests;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use GoldLapel\Amp\CachedConnection;
use GoldLapel\NativeCache;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * In-memory PostgresResult fake. PostgresResult extends Traversable via
 * the SqlResult interface, and Traversable can't be mocked directly —
 * so we hand-build a minimal IteratorAggregate that satisfies the
 * interface for the cache-handle path.
 */
final class FakePostgresResult implements PostgresResult, \IteratorAggregate
{
    /** @param list<array<string, mixed>> $rows */
    public function __construct(
        private array $rows = [],
        private ?int $colCount = null,
    ) {}

    public function getIterator(): \Iterator
    {
        foreach ($this->rows as $i => $row) {
            yield $i => $row;
        }
    }

    public function fetchRow(): ?array
    {
        return null;
    }

    public function getNextResult(): ?PostgresResult
    {
        return null;
    }

    public function getRowCount(): ?int
    {
        return count($this->rows);
    }

    public function getColumnCount(): ?int
    {
        if ($this->colCount !== null) {
            return $this->colCount;
        }
        return empty($this->rows) ? null : count(array_keys($this->rows[0]));
    }
}

/**
 * Unit tests for the async `CachedConnection.handle()` write-detection
 * behavior. Mirrors the sync `CachedPDOTest` "multi-statement write
 * detection" section.
 *
 * `detectWrite` looked at the first token only, so a single Q like
 * `SET app.tenant = 'x'; INSERT INTO orders VALUES (1)` slipped past
 * as `SET`, returned null, and the cached `SELECT * FROM orders` slot
 * would survive while the INSERT ran on the server. Fix: reuse
 * `splitStatements` via `detectWritesMulti` and union per-segment
 * invalidations.
 */
#[AllowMockObjectsWithoutExpectations]
final class CachedConnectionTest extends TestCase
{
    private NativeCache $cache;

    protected function setUp(): void
    {
        NativeCache::reset();
        $this->cache = new NativeCache();
        $this->cache->setConnected(true);
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
    }

    private function makeResult(array $rows, ?int $colCount = null): PostgresResult
    {
        return new FakePostgresResult($rows, $colCount);
    }

    private function makeCommandResult(): PostgresResult
    {
        // PostgresCommandResult.getColumnCount() returns null. Empty rows.
        return new FakePostgresResult([], null);
    }

    /**
     * Bug 1 — multi-statement SET; INSERT must invalidate the orders
     * cache slot. Use a safe-GUC name (`search_path`) so the connection
     * state hash stays stable across queries — proves the miss is from
     * invalidation, not a state-hash mismatch.
     */
    public function testQueryMultiStatementSetThenInsertInvalidatesTable(): void
    {
        $orderRows = [['id' => 1]];
        $emptyResult = $this->makeResult([], 0);

        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnOnConsecutiveCalls(
            $this->makeResult($orderRows),
            $emptyResult,
            $this->makeResult($orderRows),
        );

        $cached = new CachedConnection($real, $this->cache);

        $cached->query('SELECT * FROM orders');
        $cached->query("SET search_path = public; INSERT INTO orders VALUES (1)");
        $cached->query('SELECT * FROM orders');

        $this->assertSame(0, $this->cache->statsHits);
    }

    public function testQueryMultiStatementBeginInsertCommitInvalidatesTable(): void
    {
        $orderRows = [['id' => 1]];
        $emptyResult = $this->makeResult([], 0);

        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnOnConsecutiveCalls(
            $this->makeResult($orderRows),
            $emptyResult,
            $this->makeResult($orderRows),
        );

        $cached = new CachedConnection($real, $this->cache);

        $cached->query('SELECT * FROM orders');
        $cached->query('BEGIN; INSERT INTO orders VALUES (1); COMMIT');
        $cached->query('SELECT * FROM orders');

        $this->assertSame(0, $this->cache->statsHits);
    }

    public function testQueryMultiStatementDdlInvalidatesAll(): void
    {
        $orderRows = [['id' => 1]];

        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnOnConsecutiveCalls(
            $this->makeResult($orderRows),
            $this->makeResult([], 0),
            $this->makeResult($orderRows),
        );

        $cached = new CachedConnection($real, $this->cache);

        $cached->query('SELECT * FROM orders');
        $cached->query("SET search_path = public; CREATE TABLE other (id INT)");
        $cached->query('SELECT * FROM orders');

        $this->assertSame(0, $this->cache->statsHits);
    }

    public function testQuerySelectStillCaches(): void
    {
        // Sanity check: ordinary SELECT still caches as before. Guards
        // against an over-broad column-count gate.
        $rows = [['id' => 1, 'name' => 'alice']];

        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->once())->method('query')->willReturn(
            $this->makeResult($rows),
        );

        $cached = new CachedConnection($real, $this->cache);
        $cached->query('SELECT * FROM users');
        $cached->query('SELECT * FROM users');

        $this->assertSame(1, $this->cache->statsHits);
    }

    // --- Bug fix: SET / RESET / LISTEN responses no longer cached ---
    //
    // `query("SET …")` returns a PostgresCommandResult whose
    // getColumnCount() is null. Without a guard, the empty-row reply
    // was put in the cache, bloating the table with no-row entries
    // that never serve real data. Mirrors the JS NON_CACHEABLE_COMMANDS
    // skip and the sync CachedPDOTest equivalents.

    public function testQuerySetDoesNotPolluteCache(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturn($this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("SET search_path = public");

        $this->assertSame(0, $this->cache->size());
    }

    public function testQueryResetDoesNotPolluteCache(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturn($this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("RESET search_path");

        $this->assertSame(0, $this->cache->size());
    }

    public function testQueryListenUnlistenNotifyDoNotPolluteCache(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnCallback(fn() => $this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("LISTEN ch1");
        $cached->query("UNLISTEN ch1");
        $cached->query("NOTIFY ch1, 'hi'");

        $this->assertSame(0, $this->cache->size());
    }
}
