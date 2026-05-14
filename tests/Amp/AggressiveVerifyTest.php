<?php

namespace GoldLapel\Amp\Tests;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use GoldLapel\AggressiveVerify;
use GoldLapel\Amp\CachedConnection;
use GoldLapel\NativeCache;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Async-side aggressive-verify tests. Mirrors tests/AggressiveVerifyTest.php
 * for the sync path — covers the post-DML dml_seq bump on the Amp
 * executor + mode resolution + off-mode warning.
 *
 * Wave 5 removed the smart-auto-enable detection probe and the
 * `Amp\async()`-scheduled pg_settings reconcile. The post-DML bump is
 * now zero-RT (a counter mix into the state hash) and synchronous in
 * the wrapper, so there's no event-loop interaction to validate here.
 */
#[AllowMockObjectsWithoutExpectations]
final class AggressiveVerifyTest extends TestCase
{
    private NativeCache $cache;

    protected function setUp(): void
    {
        NativeCache::reset();
        AggressiveVerify::clearCache();
        $this->cache = new NativeCache();
        $this->cache->setConnected(true);
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
        AggressiveVerify::clearCache();
    }

    private function makeResult(array $rows, ?int $colCount = null): PostgresResult
    {
        return new FakePostgresResult($rows, $colCount);
    }

    public function testCachedConnectionRawConstructorDefaultsToAuto(): void
    {
        // Mirror of the sync raw-constructor default — always-on bump.
        $real = $this->createMock(PostgresExecutor::class);
        $cached = new CachedConnection($real, $this->cache);
        $this->assertTrue($cached->isAggressiveVerifyActive());
    }

    public function testCachedConnectionExplicitOnIsActive(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $cached = new CachedConnection($real, $this->cache, AggressiveVerify::MODE_ON);
        $this->assertTrue($cached->isAggressiveVerifyActive());
    }

    public function testCachedConnectionExplicitOffIsInactive(): void
    {
        $real = $this->createMock(PostgresExecutor::class);
        $cached = new CachedConnection($real, $this->cache, AggressiveVerify::MODE_OFF);
        // Off mode emits a one-shot warning when resolved — suppress
        // for this assertion.
        $this->assertFalse(@$cached->isAggressiveVerifyActive());
    }

    public function testCachedConnectionAutoBumpsAfterWrite(): void
    {
        // Auto mode: a DML write bumps the connection's dml_seq counter
        // with zero round-trips. No pg_trigger / pg_settings probe.
        $writeResult = $this->makeResult([], 0);
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->once())
            ->method('query')
            ->with($this->stringContains('INSERT'))
            ->willReturn($writeResult);

        $cached = new CachedConnection(
            $real,
            $this->cache,
            AggressiveVerify::MODE_AUTO,
            'amp-test-' . __METHOD__,
        );
        $beforeSeq = $cached->getGucState()->dmlSeq();
        $beforeHash = $cached->getGucState()->stateHash();
        $cached->query("INSERT INTO orders VALUES (1)");

        $this->assertSame($beforeSeq + 1, $cached->getGucState()->dmlSeq());
        $this->assertNotSame($beforeHash, $cached->getGucState()->stateHash());
    }

    public function testPostWriteVerifyAsyncIsNoopWhenInactive(): void
    {
        // Off mode → postWriteVerifyAsync is an early return; the
        // dml_seq counter stays at 0.
        $real = $this->createMock(PostgresExecutor::class);
        $cached = new CachedConnection($real, $this->cache, AggressiveVerify::MODE_OFF);
        @$cached->postWriteVerifyAsync('INSERT INTO orders VALUES (1)');
        $this->assertSame(0, $cached->getGucState()->dmlSeq());
    }
}
