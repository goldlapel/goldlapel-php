<?php

namespace GoldLapel\Tests;

use GoldLapel\AggressiveVerify;
use GoldLapel\CachedPDO;
use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Integration tests for the smart-auto-enable aggressive-verify wiring
 * across CachedPDO + GoldLapel factory. Pure-module behaviour (decision
 * precedence, caching, detector contract) lives in AggressiveVerifyTest
 * — this file exercises the actual call-path: a DML on a CachedPDO with
 * each mode runs (or doesn't run) the post-DML verify probe.
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
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
        NativeCache::reset();
    }

    protected function tearDown(): void
    {
        AggressiveVerify::clearCache();
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
        NativeCache::reset();
    }

    // ─── CachedPDO wiring ──────────────────────────────────────────────

    public function testRawCachedPdoDefaultsToOff(): void
    {
        // The raw constructor defaults to 'off' — opt-in semantics.
        // The factory path (GoldLapel::wrapPDO) defaults to 'auto'
        // and is the documented entry point. This separation lets
        // existing tests construct CachedPDO without stubbing the
        // probe.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->never())->method('query');
        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache);
        $this->assertFalse($cached->isAggressiveVerifyActive());
    }

    public function testCachedPdoOnRunsPostWriteVerify(): void
    {
        // Mode 'on' bypasses detection — postWriteVerify runs after a
        // DML write, issuing the pg_settings probe.
        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.user_id', 'setting' => '99'],
        ]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->with($this->stringContains('INSERT'))
            ->willReturn(1);
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_settings'))
            ->willReturn($verifyStmt);

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $cached->exec("INSERT INTO orders VALUES (1)");

        // postWriteVerify applied — the GUC state hash now reflects
        // app.user_id=99 (matching what verify returned).
        $this->assertNotSame('0', $cached->getGucState()->stateHash());
    }

    public function testCachedPdoOffSkipsPostWriteVerify(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->willReturn(1);
        // No query() call — verify never fires when mode is 'off'.
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_OFF);
        $cached->exec("INSERT INTO orders VALUES (1)");
    }

    public function testCachedPdoAutoTriggersDetectionOnFirstWrite(): void
    {
        // In auto mode, a write triggers the detection probe; if the
        // probe returns false, postWriteVerify is skipped.
        $detectionStmt = $this->createStub(\PDOStatement::class);
        $detectionStmt->method('fetch')->willReturn(['present' => false]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->willReturn(1);
        // ONE query call — the detection probe. No verify probe because
        // detection said "no triggers found".
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_trigger'))
            ->willReturn($detectionStmt);

        $cache = new NativeCache();
        $cached = new CachedPDO(
            $pdo,
            $cache,
            AggressiveVerify::MODE_AUTO,
            'unique-test-key-' . __METHOD__,
        );
        $cached->exec("INSERT INTO orders VALUES (1)");
        $this->assertFalse($cached->isAggressiveVerifyActive());
    }

    public function testCachedPdoAutoEnablesWhenDetectionSucceeds(): void
    {
        // Detection finds a trigger → auto enables → verify probe fires
        // after the write.
        $detectionStmt = $this->createStub(\PDOStatement::class);
        $detectionStmt->method('fetch')->willReturn(['present' => true]);
        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.tenant', 'setting' => 'acme'],
        ]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->willReturn(1);
        // Two query() calls: detection probe + verify probe.
        $pdo->expects($this->exactly(2))
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($detectionStmt, $verifyStmt) {
                if (str_contains($sql, 'pg_trigger')) {
                    return $detectionStmt;
                }
                if (str_contains($sql, 'pg_settings')) {
                    return $verifyStmt;
                }
                $this->fail("unexpected query: {$sql}");
            });

        $cache = new NativeCache();
        $cached = new CachedPDO(
            $pdo,
            $cache,
            AggressiveVerify::MODE_AUTO,
            'unique-test-key-' . __METHOD__,
        );
        $cached->exec("INSERT INTO orders VALUES (1)");

        $this->assertTrue($cached->isAggressiveVerifyActive());
        // Verify result was applied.
        $this->assertNotSame('0', $cached->getGucState()->stateHash());
    }

    public function testCachedPdoAutoDetectionRunsOncePerKey(): void
    {
        // Detection should fire on FIRST write, not subsequent writes —
        // the AggressiveVerify static cache short-circuits.
        $detectionStmt = $this->createStub(\PDOStatement::class);
        $detectionStmt->method('fetch')->willReturn(['present' => false]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->exactly(2))->method('exec')->willReturn(1);
        // Exactly ONE query() call across two writes — detection cached.
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_trigger'))
            ->willReturn($detectionStmt);

        $cache = new NativeCache();
        $cached = new CachedPDO(
            $pdo,
            $cache,
            AggressiveVerify::MODE_AUTO,
            'unique-test-key-' . __METHOD__,
        );
        $cached->exec("INSERT INTO orders VALUES (1)");
        $cached->exec("INSERT INTO orders VALUES (2)");
    }

    public function testCachedPdoAutoSkipsVerifyForDdl(): void
    {
        // DDL invalidates the whole cache; postWriteVerify is skipped
        // because detectWritesMulti returns DDL_SENTINEL. Even with
        // mode 'on', DDL doesn't probe.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('exec')->willReturn(0);
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $cached->exec("CREATE TABLE foo (id INT)");
    }

    public function testCachedPdoOnVerifyFailureMarksDirty(): void
    {
        // Probe failure → connection marked dirty for the next acquire.
        // The user's write completes normally; verify is best-effort.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->willReturn(1);
        $pdo->expects($this->once())
            ->method('query')
            ->willThrowException(new \PDOException('boom'));

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $cached->exec("INSERT INTO orders VALUES (1)");
        $this->assertTrue($cached->getGucState()->isDirty());
    }

    public function testCachedPdoSkipsVerifyInsideTransaction(): void
    {
        // Inside an open transaction, postWriteVerify is skipped — the
        // tx-end path's verify-on-checkout will handle any state
        // changes after COMMIT.
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->expects($this->once())->method('exec')->willReturn(1);
        // Only the BEGIN's transition counts; no verify probe should
        // fire while inTransaction=true.
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $cached = new CachedPDO($pdo, $cache, AggressiveVerify::MODE_ON);
        $cached->beginTransaction();
        $cached->exec("INSERT INTO orders VALUES (1)");
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
        // — the documented factory path uses this default.
        $detectionStmt = $this->createStub(\PDOStatement::class);
        $detectionStmt->method('fetch')->willReturn(['present' => false]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())->method('exec')->willReturn(1);
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_trigger'))
            ->willReturn($detectionStmt);

        $cached = \GoldLapel\GoldLapel::wrapPDOStatic(
            $pdo,
            7934, // arbitrary invalidation port
            null,
            AggressiveVerify::MODE_AUTO,
            'factory-default-test',
        );
        $cached->exec("INSERT INTO orders VALUES (1)");
        $this->assertFalse($cached->isAggressiveVerifyActive());
    }
}
