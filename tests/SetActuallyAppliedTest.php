<?php

namespace GoldLapel\Tests;

use GoldLapel\CachedPDO;
use GoldLapel\ConnectionGucState;
use GoldLapel\NativeCache;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Wave 2 — SET-actually-applied check.
 *
 * Wrapper used to mutate the per-connection unsafe-GUC state hash
 * optimistically as soon as it observed a SET on the wire. If the real
 * PDO call then threw a PDOException (or returned `false` in
 * ERRMODE_SILENT), the wrapper's state would diverge from the server.
 * Subsequent reads would key on a state hash the server never reached,
 * potentially serving cached rows from a different RLS principal.
 *
 * Fix: snapshot before observe, restore + markDirty on failure. Snapshot
 * the same way at BEGIN to revert non-LOCAL SETs on ROLLBACK (PG
 * tx-scoped revert).
 *
 * This file mirrors goldlapel-js's set-actually-applied.test.js
 * coverage: success / error / multi-stmt prefix / BEGIN+ROLLBACK paths.
 */
#[AllowMockObjectsWithoutExpectations]
class SetActuallyAppliedTest extends TestCase
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

    private function makeMockPDO(): \PDO
    {
        return $this->createMock(\PDO::class);
    }

    private function makeMockStmt(array $rows = [], int $colCount = 0): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn($rows);
        $stmt->method('columnCount')->willReturn($colCount > 0 ? $colCount : (empty($rows) ? 0 : count(array_keys($rows[0]))));
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(count($rows));
        return $stmt;
    }

    // ─── 1. Success path keeps the observed mutation ────────────────────

    public function testSetOnSuccessAppliesToStateHash(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));

        $cached = new CachedPDO($pdo, $this->cache);
        $hashBefore = $cached->getGucState()->stateHash();
        $this->assertSame('0', $hashBefore);

        $cached->query("SET app.user_id = '42'");

        $hashAfter = $cached->getGucState()->stateHash();
        $this->assertNotSame('0', $hashAfter, 'SET on the wire mutates state hash on success');
    }

    public function testSetExecOnSuccessAppliesToStateHash(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturn(0);

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->exec("SET app.user_id = '42'");

        $this->assertNotSame('0', $cached->getGucState()->stateHash());
    }

    // ─── 2. PDOException reverts the mutation ───────────────────────────

    public function testSetThatThrowsLeavesStateUnchanged(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willThrowException(new \PDOException('boom'));

        $cached = new CachedPDO($pdo, $this->cache);
        $hashBefore = $cached->getGucState()->stateHash();

        try {
            $cached->query("SET app.user_id = '42'");
            $this->fail('Expected PDOException to propagate');
        } catch (\PDOException $e) {
            // Exception SHOULD propagate — wrapper does not swallow.
            $this->assertSame('boom', $e->getMessage());
        }

        $this->assertSame(
            $hashBefore,
            $cached->getGucState()->stateHash(),
            'Failed SET must not mutate the wrapper-side state hash',
        );
        $this->assertTrue(
            $cached->getGucState()->isDirty(),
            'Failed SET marks the connection dirty so the next acquire reverifies',
        );
    }

    public function testSetExecThatThrowsLeavesStateUnchanged(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willThrowException(new \PDOException('exec boom'));

        $cached = new CachedPDO($pdo, $this->cache);
        try {
            $cached->exec("SET app.user_id = '42'");
            $this->fail('Expected PDOException');
        } catch (\PDOException) {
        }

        $this->assertSame('0', $cached->getGucState()->stateHash());
        $this->assertTrue($cached->getGucState()->isDirty());
    }

    public function testSetThatReturnsFalseLeavesStateUnchanged(): void
    {
        // ERRMODE_SILENT / ERRMODE_WARNING path — query returns false
        // instead of throwing. Wrapper must still revert + mark dirty.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn(false);

        $cached = new CachedPDO($pdo, $this->cache);
        $r = $cached->query("SET app.user_id = '42'");

        $this->assertFalse($r);
        $this->assertSame('0', $cached->getGucState()->stateHash());
        $this->assertTrue($cached->getGucState()->isDirty());
    }

    public function testSetExecThatReturnsFalseLeavesStateUnchanged(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturn(false);

        $cached = new CachedPDO($pdo, $this->cache);
        $r = $cached->exec("SET app.user_id = '42'");

        $this->assertFalse($r);
        $this->assertSame('0', $cached->getGucState()->stateHash());
        $this->assertTrue($cached->getGucState()->isDirty());
    }

    // ─── 3. Pre-existing state is preserved on failure ──────────────────

    public function testFailedSetDoesNotClobberPriorState(): void
    {
        // First SET succeeds → state has app.user_id='alice'.
        // Second SET (different value) throws → state stays at 'alice'.
        $pdo = $this->makeMockPDO();
        $pdo->expects($this->exactly(2))
            ->method('query')
            ->willReturnCallback(function ($sql) {
                if (str_contains($sql, "'bob'")) {
                    throw new \PDOException('rejected');
                }
                return $this->makeMockStmt([], 0);
            });

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->query("SET app.user_id = 'alice'");
        $aliceHash = $cached->getGucState()->stateHash();
        $this->assertNotSame('0', $aliceHash);

        try {
            $cached->query("SET app.user_id = 'bob'");
            $this->fail('Expected PDOException');
        } catch (\PDOException) {
        }

        // Hash must still be the alice hash, not the bob hash, not the
        // empty hash.
        $this->assertSame(
            $aliceHash,
            $cached->getGucState()->stateHash(),
            'Failed second SET must not clobber successful first SET',
        );
    }

    // ─── 4. Multi-statement Q error path ────────────────────────────────

    public function testMultiStmtSetThatErrorsRevertsAndMarksDirty(): void
    {
        // `SET app.user_id = '42'; SELECT 1/0` — the SET would have
        // applied server-side before the divide-by-zero, but since we
        // can't tell from a single PDOException which segment failed,
        // the safe answer is restore + markDirty so the next acquire
        // reverifies from pg_settings.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willThrowException(new \PDOException('division by zero'));

        $cached = new CachedPDO($pdo, $this->cache);
        try {
            $cached->query("SET app.user_id = '42'; SELECT 1/0");
            $this->fail('Expected PDOException');
        } catch (\PDOException) {
        }

        $this->assertSame(
            '0',
            $cached->getGucState()->stateHash(),
            'Multi-stmt error reverts wrapper-side state',
        );
        $this->assertTrue(
            $cached->getGucState()->isDirty(),
            'Verify-on-checkout will resync the actual server state on next acquire',
        );
    }

    // ─── 5. BEGIN + ROLLBACK reverts the SET ────────────────────────────

    public function testBeginSetRollbackRevertsState(): void
    {
        // PG semantics: a non-LOCAL `SET app.x = 'y'` issued inside an
        // aborted transaction is reverted by the server. The wrapper
        // must follow.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));
        $pdo->method('exec')->willReturn(0);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('rollBack')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->beginTransaction();
        $cached->exec("SET app.user_id = '42'");
        // Inside-tx: cache is bypassed; state hash mutates locally
        // (matters for query keying after the tx ends).
        $this->assertNotSame('0', $cached->getGucState()->stateHash());

        $cached->rollBack();

        $this->assertSame(
            '0',
            $cached->getGucState()->stateHash(),
            'ROLLBACK reverts non-LOCAL SETs issued inside the tx',
        );
    }

    public function testBeginSetCommitKeepsState(): void
    {
        // Sanity check: COMMIT keeps the SET (counterpart to the
        // ROLLBACK test above).
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));
        $pdo->method('exec')->willReturn(0);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('commit')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->beginTransaction();
        $cached->exec("SET app.user_id = '42'");
        $hashInTx = $cached->getGucState()->stateHash();
        $cached->commit();

        $this->assertSame(
            $hashInTx,
            $cached->getGucState()->stateHash(),
            'COMMIT keeps non-LOCAL SETs',
        );
    }

    public function testInlineBeginSetRollbackInSingleQRevertsState(): void
    {
        // Single multi-statement Q wrapping the whole tx: `BEGIN; SET
        // app.x = 'y'; ROLLBACK`. Wrapper sees this via observeSql + the
        // multi-statement tx-boundary walker, so the ROLLBACK final
        // segment must trigger the state restore.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->query("BEGIN; SET app.user_id = '42'; ROLLBACK");

        $this->assertSame(
            '0',
            $cached->getGucState()->stateHash(),
            'Inline BEGIN; SET; ROLLBACK reverts via multi-stmt boundary walker',
        );
        $this->assertFalse($cached->inTransaction());
    }

    public function testInlineBeginSetCommitInSingleQKeepsState(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->query("BEGIN; SET app.user_id = '42'; COMMIT");

        $this->assertNotSame(
            '0',
            $cached->getGucState()->stateHash(),
            'Inline BEGIN; SET; COMMIT keeps state via multi-stmt walker',
        );
    }

    public function testBeginSetRollbackPreservesPreTxState(): void
    {
        // Pre-existing app.user_id = 'admin', tx sets it to '42' then
        // ROLLBACK. State should snap back to 'admin', not zero.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));
        $pdo->method('exec')->willReturn(0);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('rollBack')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->exec("SET app.user_id = 'admin'");
        $adminHash = $cached->getGucState()->stateHash();
        $this->assertNotSame('0', $adminHash);

        $cached->beginTransaction();
        $cached->exec("SET app.user_id = '42'");
        $cached->rollBack();

        $this->assertSame(
            $adminHash,
            $cached->getGucState()->stateHash(),
            'ROLLBACK restores the pre-BEGIN state, not the empty state',
        );
    }

    public function testFailedSelectDoesNotMarkDirty(): void
    {
        // Failed SELECT (no SET observed) shouldn't trigger a dirty
        // flag — there's nothing to reverify, so the next acquire
        // shouldn't pay the pg_settings round-trip.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willThrowException(new \PDOException('table not found'));

        $cached = new CachedPDO($pdo, $this->cache);
        try {
            $cached->query('SELECT * FROM nonexistent');
            $this->fail('Expected PDOException');
        } catch (\PDOException) {
        }

        $this->assertFalse(
            $cached->getGucState()->isDirty(),
            'Failed SELECT (no SET observed) does not mark dirty',
        );
    }

    public function testInlineBeginSetRollbackPreservesPreTxState(): void
    {
        // Same as above but inline single Q.
        $pdo = $this->makeMockPDO();
        $pdo->method('query')->willReturn($this->makeMockStmt([], 0));
        $pdo->method('exec')->willReturn(0);

        $cached = new CachedPDO($pdo, $this->cache);
        $cached->exec("SET app.user_id = 'admin'");
        $adminHash = $cached->getGucState()->stateHash();

        $cached->query("BEGIN; SET app.user_id = '42'; ROLLBACK");

        $this->assertSame(
            $adminHash,
            $cached->getGucState()->stateHash(),
            'Inline BEGIN; SET; ROLLBACK restores pre-tx state',
        );
    }

    // ─── 6. Snapshot/restore primitives ─────────────────────────────────

    public function testSnapshotRestoreRoundTrip(): void
    {
        $state = new ConnectionGucState();
        $state->observeSql("SET app.user_id = '7'");
        $hashWithSeven = $state->stateHash();

        $snap = $state->snapshot();

        $state->observeSql("SET app.user_id = '8'");
        $this->assertNotSame($hashWithSeven, $state->stateHash());

        $state->restore($snap);
        $this->assertSame($hashWithSeven, $state->stateHash());
    }

    public function testSnapshotPreservesDirtyFlag(): void
    {
        $state = new ConnectionGucState();
        $state->markDirty();
        $this->assertTrue($state->isDirty());

        $snap = $state->snapshot();
        $state->clearDirty();
        $this->assertFalse($state->isDirty());

        $state->restore($snap);
        $this->assertTrue($state->isDirty());
    }

    // ─── 7. ROLLBACK detection ──────────────────────────────────────────

    public function testIsTxRollbackDistinguishesFromCommit(): void
    {
        $this->assertTrue(NativeCache::isTxRollback('ROLLBACK'));
        $this->assertTrue(NativeCache::isTxRollback('rollback'));
        $this->assertTrue(NativeCache::isTxRollback('  ROLLBACK  '));
        $this->assertTrue(NativeCache::isTxRollback('ROLLBACK WORK'));
        $this->assertTrue(NativeCache::isTxRollback('ROLLBACK TRANSACTION'));

        $this->assertFalse(NativeCache::isTxRollback('COMMIT'));
        $this->assertFalse(NativeCache::isTxRollback('END'));

        // ROLLBACK TO SAVEPOINT is a partial revert we don't model.
        $this->assertFalse(NativeCache::isTxRollback('ROLLBACK TO SAVEPOINT s1'));
    }

    public function testBodyEndsWithRollbackForMultiStmt(): void
    {
        $this->assertTrue(NativeCache::bodyEndsWithRollback('BEGIN; SELECT 1; ROLLBACK'));
        $this->assertFalse(NativeCache::bodyEndsWithRollback('BEGIN; SELECT 1; COMMIT'));
        // Single-statement ROLLBACK
        $this->assertTrue(NativeCache::bodyEndsWithRollback('ROLLBACK'));
        // No tx boundary at all
        $this->assertFalse(NativeCache::bodyEndsWithRollback('SELECT 1'));
    }
}
