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

    // --- Bug fix: tx-flag bookkeeping for multi-statement BEGIN/COMMIT ---
    //
    // Pre-fix, a single Q like `BEGIN; LISTEN foo; COMMIT` flipped the
    // wrapper-side inTransaction flag based on the first token only —
    // server ends out-of-tx, wrapper thinks still in tx. Cache bypass
    // forever until a fresh BEGIN/COMMIT cycle reset state. The fix
    // walks every segment via applyTxBoundaries(). Mirrors goldlapel-js
    // commit 0d19816.

    /**
     * Reach into the private inTransaction property for assertion. The
     * class doesn't expose a getter (matches the JS reference's internal
     * `_inTransaction`), and these regression tests are validating that
     * internal flag matches the server's view.
     */
    private function readInTransaction(CachedConnection $c): bool
    {
        $ref = new \ReflectionClass($c);
        $prop = $ref->getProperty('inTransaction');
        return $prop->getValue($c);
    }

    public function testQueryMultiStatementBeginCommitNoWriteEndsOutOfTx(): void
    {
        // BEGIN; LISTEN foo; COMMIT — no write detection, must still
        // walk segments to land on inTransaction=false.
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturn($this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query('BEGIN; LISTEN foo; COMMIT');

        $this->assertFalse($this->readInTransaction($cached));
    }

    public function testQueryMultiStatementBeginNoCommitStaysInTx(): void
    {
        // BEGIN; SELECT 1 — no closing COMMIT, server still in tx.
        $rows = [['?column?' => 1]];

        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturn($this->makeResult($rows));

        $cached = new CachedConnection($real, $this->cache);
        $cached->query('BEGIN; SELECT 1');

        $this->assertTrue($this->readInTransaction($cached));
    }

    public function testQueryMultiStatementBeginRollbackEndsOutOfTx(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturn($this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query('BEGIN; SELECT 1; ROLLBACK');

        $this->assertFalse($this->readInTransaction($cached));
    }

    public function testQueryPostMultiStatementCommitStillCaches(): void
    {
        // The user-visible regression: pre-fix, a `BEGIN; LISTEN foo;
        // COMMIT` body left inTransaction stuck at true, so every
        // subsequent SELECT bypassed the cache forever. Post-fix, the
        // SELECT after the body must be cacheable.
        $rows = [['id' => 1]];

        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnOnConsecutiveCalls(
            $this->makeCommandResult(),
            $this->makeResult($rows),
        );

        $cached = new CachedConnection($real, $this->cache);
        $cached->query('BEGIN; LISTEN foo; COMMIT');
        $cached->query('SELECT * FROM orders');
        $cached->query('SELECT * FROM orders');

        $this->assertSame(1, $this->cache->statsHits);
        $this->assertFalse($this->readInTransaction($cached));
    }

    public function testQueryCloseThenReopenEndsInTx(): void
    {
        // Walking segments in order: COMMIT closes, then BEGIN reopens —
        // final state is `in tx`. Guards against last-segment-only impls.
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturn($this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query('COMMIT; BEGIN');

        $this->assertTrue($this->readInTransaction($cached));
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

    // ─── 2026-05-05 RLS hardening: dirty / verify on the async path ────

    public function testVerifyIfDirtyOnCleanIsNoop(): void
    {
        // A clean connection's verifyIfDirty() returns false without
        // touching the underlying executor.
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->never())->method('query');
        $cached = new CachedConnection($real, $this->cache);
        $this->assertFalse($cached->verifyIfDirty());
    }

    public function testVerifyIfDirtyRebuildsStateFromPgSettings(): void
    {
        // Mark dirty, call verifyIfDirty, expect the executor to receive
        // the pg_settings query and the resulting rows to feed into
        // ConnectionGucState.
        $verifyResult = $this->makeResult([
            ['name' => 'app.user_id', 'setting' => '7'],
            ['name' => 'work_mem', 'setting' => '4MB'], // safe — filtered
        ]);
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_settings'))
            ->willReturn($verifyResult);

        $cached = new CachedConnection($real, $this->cache);
        $cached->markStateDirty();
        $this->assertTrue($cached->getGucState()->isDirty());
        $ok = $cached->verifyIfDirty();
        $this->assertTrue($ok);
        $this->assertFalse($cached->getGucState()->isDirty());
        // State should match what an in-band SET would have produced.
        $other = new \GoldLapel\ConnectionGucState();
        $other->observeSql("SET app.user_id = '7'");
        $this->assertSame($other->stateHash(), $cached->getGucState()->stateHash());
    }

    public function testVerifyIfDirtyKeepsDirtyOnExecutorFailure(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->once())
            ->method('query')
            ->willThrowException(new \RuntimeException('bad'));

        $cached = new CachedConnection($real, $this->cache);
        $cached->markStateDirty();
        $ok = $cached->verifyIfDirty();
        $this->assertFalse($ok);
        $this->assertTrue($cached->getGucState()->isDirty());
    }

    public function testQueryRunsVerifyIfDirtyBeforeUserQuery(): void
    {
        // Dirty connection: query() should fire the verify pass first,
        // then the user's read.
        $verifyResult = $this->makeResult([
            ['name' => 'app.user_id', 'setting' => 'A'],
        ]);
        $userResult = $this->makeResult([['id' => 1]]);

        $real = $this->createMock(PostgresExecutor::class);
        $matcher = $this->exactly(2);
        $real->expects($matcher)
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($matcher, $verifyResult, $userResult) {
                if ($matcher->numberOfInvocations() === 1) {
                    $this->assertStringContainsString('pg_settings', $sql);
                    return $verifyResult;
                }
                $this->assertSame('SELECT 1', $sql);
                return $userResult;
            });

        $cached = new CachedConnection($real, $this->cache);
        $cached->markStateDirty();
        $cached->query('SELECT 1');

        $other = new \GoldLapel\ConnectionGucState();
        $other->observeSql("SET app.user_id = 'A'");
        $this->assertSame($other->stateHash(), $cached->getGucState()->stateHash());
    }

    public function testDiscardAllClearsStateInAsyncPath(): void
    {
        // DISCARD ALL on the async wrapper clears state map AND drops
        // the dirty flag (mirrors the sync test).
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnCallback(fn() => $this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("SET app.user_id = '42'");
        $this->assertNotSame('0', $cached->getGucState()->stateHash());

        $cached->markStateDirty();
        $this->assertTrue($cached->getGucState()->isDirty());

        $cached->query('DISCARD ALL');
        $this->assertSame('0', $cached->getGucState()->stateHash());
        $this->assertFalse($cached->getGucState()->isDirty());
    }

    public function testSetConfigCallMutatesAsyncState(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnCallback(fn() => $this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("SELECT set_config('app.user_id', '99', false)");

        $other = new \GoldLapel\ConnectionGucState();
        $other->observeSql("SET app.user_id = '99'");
        $this->assertSame($other->stateHash(), $cached->getGucState()->stateHash());
    }

    public function testSetConfigLocalDoesNotMutateAsyncState(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnCallback(fn() => $this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("SELECT set_config('app.user_id', '99', true)");

        $this->assertSame('0', $cached->getGucState()->stateHash());
    }

    public function testFormattingGucMutatesAsyncState(): void
    {
        // 2026-05-05 classifier expansion: SET timezone now mutates state.
        $real = $this->createMock(PostgresExecutor::class);
        $real->method('query')->willReturnCallback(fn() => $this->makeCommandResult());

        $cached = new CachedConnection($real, $this->cache);
        $cached->query("SET timezone = 'UTC'");
        $this->assertNotSame('0', $cached->getGucState()->stateHash());
    }
}
