<?php

namespace GoldLapel\Tests;

use GoldLapel\AggressiveVerify;
use GoldLapel\CachedPDO;
use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Integration tests for the post-DML dml_seq-bump wiring across
 * CachedPDO + GoldLapel factory. Pure-module behaviour (mode parsing,
 * off-warning dedup) lives in AggressiveVerifyTest — this file
 * exercises the actual call-path: a DML on a CachedPDO with each mode
 * bumps (or doesn't bump) the connection's per-connection dml_seq
 * counter.
 *
 * Test naming: "CachedPdo*" tests target src/CachedPDO.php's exec /
 * query / prepare-execute paths; "Factory*" tests target the option
 * surface on GoldLapel::__construct + wrapPDOStatic.
 */
class AggressiveVerifyWiringTest extends TestCase
{
    protected function setUp(): void
    {
        AggressiveVerify::clearCache();
        NativeCache::reset();
    }

    protected function tearDown(): void
    {
        AggressiveVerify::clearCache();
        NativeCache::reset();
    }

    // ─── CachedPDO wiring ──────────────────────────────────────────────

    public function testRawCachedPdoDefaultsToAuto(): void
    {
        // Default mode is 'auto' across the board — the bump is
        // zero-RT so there's no upside to defaulting off. Raw
        // constructor matches factory.
        $pdo = $this->createStub(\PDO::class);
        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache);
        $this->assertTrue($cached->isAggressiveVerifyActive());
    }

    public function testCachedPdoOnBumpsDmlSeq(): void
    {
        // Mode 'on' → postWriteVerify bumps dml_seq after a DML write,
        // mixing into the connection's state hash so any subsequent
        // cacheable read can't share a slot with a pre-DML read.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->with($this->stringContains('INSERT'))
            ->willReturn(1);
        // No pg_settings probe — Wave 5 removed it.
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $beforeSeq = $cached->getGucState()->dmlSeq();
        $beforeHash = $cached->getGucState()->stateHash();
        $cached->exec("INSERT INTO orders VALUES (1)");

        // dml_seq advanced; state hash rolled forward.
        $this->assertSame($beforeSeq + 1, $cached->getGucState()->dmlSeq());
        $this->assertNotSame($beforeHash, $cached->getGucState()->stateHash());
    }

    public function testCachedPdoOffSkipsBump(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->willReturn(1);
        // No verify probe AND no pg_settings query — off mode is a
        // pure no-op.
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_OFF);

        // Off-mode emits a one-shot warning when the decision is first
        // resolved. Suppress for this assertion path.
        @$cached->exec("INSERT INTO orders VALUES (1)");

        $this->assertSame(0, $cached->getGucState()->dmlSeq());
        $this->assertSame('0', $cached->getGucState()->stateHash());
    }

    public function testCachedPdoAutoBumpsOnEveryDml(): void
    {
        // Auto mode is always-on — no detection probe, just bump.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->exactly(3))->method('exec')->willReturn(1);
        // ZERO query() calls — no detection probe, no pg_settings.
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO(
            $pdo,
            $cache,
            AggressiveVerify::MODE_AUTO,
            'unique-test-key-' . __METHOD__,
        );
        $cached->exec("INSERT INTO orders VALUES (1)");
        $cached->exec("INSERT INTO orders VALUES (2)");
        $cached->exec("INSERT INTO orders VALUES (3)");

        $this->assertSame(3, $cached->getGucState()->dmlSeq());
        $this->assertTrue($cached->isAggressiveVerifyActive());
    }

    public function testCachedPdoAutoSkipsBumpForDdl(): void
    {
        // DDL invalidates the whole cache; postWriteVerify is skipped
        // because detectWritesMulti returns DDL_SENTINEL. Even with
        // mode 'on', DDL doesn't bump.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('exec')->willReturn(0);
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $cached->exec("CREATE TABLE foo (id INT)");

        $this->assertSame(0, $cached->getGucState()->dmlSeq());
    }

    public function testCachedPdoSkipsBumpInsideTransaction(): void
    {
        // Inside an open transaction, postWriteVerify is skipped — the
        // cache is bypassed anyway and any non-LOCAL SET issued by a
        // trigger surfaces at COMMIT (verify-on-checkout reconciles
        // via DISCARD or explicit clearDirty on connection reuse).
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('exec')->willReturn(1);
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $cached->beginTransaction();
        $cached->exec("INSERT INTO orders VALUES (1)");

        $this->assertSame(0, $cached->getGucState()->dmlSeq());
    }

    public function testCachedPdoCacheMissAfterDmlOnDifferentSlot(): void
    {
        // The whole point: a SELECT issued BEFORE a DML on the same
        // connection caches under one state hash; a SELECT issued
        // AFTER the DML routes to a different slot (because dml_seq
        // bumped the hash). This is the cache-key isolation that
        // closes the trigger-internal-SET gap.
        $cache = new NativeCache();
        $cache->setConnected(true);

        $preDmlStmt = $this->createStub(\PDOStatement::class);
        $preDmlStmt->method('fetchAll')->willReturn([['v' => 1]]);

        $postDmlStmt = $this->createStub(\PDOStatement::class);
        $postDmlStmt->method('fetchAll')->willReturn([['v' => 2]]);

        $pdo = $this->createMock(\PDO::class);
        $callCount = 0;
        $pdo->expects($this->exactly(2))
            ->method('query')
            ->willReturnCallback(function () use ($preDmlStmt, $postDmlStmt, &$callCount) {
                $callCount++;
                return $callCount === 1 ? $preDmlStmt : $postDmlStmt;
            });
        $pdo->expects($this->once())->method('exec')->willReturn(1);

        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);

        // First SELECT — caches under pre-DML state hash.
        $cached->query("SELECT v FROM accounts WHERE id = 1");

        // DML — bumps dml_seq, rolling the state hash forward.
        $cached->exec("INSERT INTO orders VALUES (1)");

        // Second SELECT — different state hash, so the cache miss
        // forces another real query() call. The pre-DML cached row
        // (v=1) cannot leak.
        $cached->query("SELECT v FROM accounts WHERE id = 1");

        // Both real query() calls fired (callCount asserted above).
        $this->assertSame(2, $callCount);
    }

    // ─── Dirty flag → cache bypass ─────────────────────────────────────

    public function testDirtyFlagBypassesCacheRead(): void
    {
        $cache = new NativeCache();
        $cache->setConnected(true);

        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([['v' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        // Two query() calls — both go to the real PDO because the
        // connection is dirty for both. Without dirty bypass, the
        // second would hit the cache.
        $pdo->expects($this->exactly(2))->method('query')->willReturn($stmt);

        $cached = new CachedPDO($pdo, $cache);

        // Mark dirty BEFORE the first query — both queries bypass.
        $cached->markStateDirty();
        $cached->query("SELECT v FROM accounts");
        // Still dirty (dirty flag isn't cleared on read).
        $this->assertTrue($cached->getGucState()->isDirty());
        $cached->query("SELECT v FROM accounts");
    }

    public function testDirtyFlagBypassesCachePut(): void
    {
        // While dirty, results are NOT seeded into the cache — we
        // don't know the real server state, so a put would risk
        // serving wrong rows to future cache hits on the same hash.
        $cache = new NativeCache();
        $cache->setConnected(true);

        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([['v' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('query')->willReturn($stmt);

        $cached = new CachedPDO($pdo, $cache);
        $cached->markStateDirty();
        $cached->query("SELECT v FROM accounts");

        // Cache size should be 0 — nothing was put.
        $this->assertSame(0, $cache->size());
    }

    public function testCleanConnectionCachesNormally(): void
    {
        // Sanity check the dirty-bypass guard doesn't break the
        // happy path: a clean connection caches as usual.
        $cache = new NativeCache();
        $cache->setConnected(true);

        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([['v' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        // Only ONE query() — second call should hit the cache.
        $pdo->expects($this->once())->method('query')->willReturn($stmt);

        $cached = new CachedPDO($pdo, $cache);
        $cached->query("SELECT v FROM accounts");
        $cached->query("SELECT v FROM accounts");
    }

    public function testDiscardClearsDirtyAndRestoresCacheReads(): void
    {
        // DISCARD ALL is the universal "state is now default" signal —
        // it should clear dirty + reset dml_seq, restoring cache
        // participation.
        $cache = new NativeCache();
        $cache->setConnected(true);

        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([['v' => 1]]);
        $discardStmt = $this->createStub(\PDOStatement::class);
        $discardStmt->method('fetchAll')->willReturn([]);
        $discardStmt->method('columnCount')->willReturn(0);

        $pdo = $this->createMock(\PDO::class);
        // SELECT (cache miss, dirty bypass) + DISCARD ALL + SELECT
        // (cache miss, fresh state hash) + SELECT (cache HIT). So
        // 3 query() calls total — one of them is the DISCARD.
        $pdo->expects($this->exactly(3))
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($stmt, $discardStmt) {
                if (stripos($sql, 'DISCARD') !== false) {
                    return $discardStmt;
                }
                return $stmt;
            });

        $cached = new CachedPDO($pdo, $cache);
        $cached->markStateDirty();
        $cached->query("SELECT v FROM accounts");
        $this->assertTrue($cached->getGucState()->isDirty());

        $cached->query("DISCARD ALL");
        $this->assertFalse($cached->getGucState()->isDirty());

        // First post-DISCARD query — cache miss (different state-hash
        // path internally), populates cache.
        $cached->query("SELECT v FROM accounts");
        // Second post-DISCARD query — cache HIT (no further query()
        // call expected).
        $cached->query("SELECT v FROM accounts");
    }

    // ─── Factory wiring ────────────────────────────────────────────────

    public function testFactoryStartRejectsBadAggressiveVerifyValue(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/aggressive_verify/');
        new \GoldLapel\GoldLapel('postgres://localhost/db', [
            'aggressive_verify' => 'maybe',
        ]);
    }

    public function testFactoryAcceptsValidAggressiveVerifyValues(): void
    {
        // Valid values don't raise — we only construct, not start, so
        // there's no spawn dependency.
        foreach (['auto', 'on', 'off', 'AUTO', 'On'] as $value) {
            $gl = new \GoldLapel\GoldLapel('postgres://localhost/db', [
                'aggressive_verify' => $value,
            ]);
            $this->assertInstanceOf(\GoldLapel\GoldLapel::class, $gl);
        }
    }

    public function testFactoryWrapPdoStaticDefaultsToAuto(): void
    {
        // wrapPDOStatic with no aggressive-verify arg defaults to 'auto'
        // — a DML on the resulting CachedPDO bumps dml_seq.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('exec')->willReturn(1);
        // No pg_settings query — Wave 5 removed the probe.
        $pdo->expects($this->never())->method('query');

        $cached = \GoldLapel\GoldLapel::wrapPDOStatic(
            $pdo,
            7934, // arbitrary invalidation port
            null,
            AggressiveVerify::MODE_AUTO,
            'factory-default-test',
        );
        $cached->exec("INSERT INTO orders VALUES (1)");
        $this->assertTrue($cached->isAggressiveVerifyActive());
        $this->assertSame(1, $cached->getGucState()->dmlSeq());
    }
}
