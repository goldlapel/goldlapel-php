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
 * for the sync path — covers detection-on-first-write, override
 * precedence, and the async post-DML verify scheduling via Amp\async().
 *
 * Async verify uses Amp\async(), which schedules the probe in a sibling
 * coroutine. From an event-loop-less unit test we drive the loop by
 * calling \Amp\Future\await on a synthetic future or by running inside
 * an explicit \Revolt\EventLoop::run() context. The simpler model here
 * is to assert that postWriteVerifyAsync() is wired AT ALL — the event
 * loop integration tests in IntegrationTest.php exercise the actual
 * fiber scheduling.
 */
#[AllowMockObjectsWithoutExpectations]
final class AggressiveVerifyTest extends TestCase
{
    private NativeCache $cache;

    protected function setUp(): void
    {
        NativeCache::reset();
        AggressiveVerify::clearCache();
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
        $this->cache = new NativeCache();
        $this->cache->setConnected(true);
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
        AggressiveVerify::clearCache();
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
    }

    private function makeResult(array $rows, ?int $colCount = null): PostgresResult
    {
        return new FakePostgresResult($rows, $colCount);
    }

    public function testCachedConnectionRawConstructorDefaultsToOff(): void
    {
        // Mirror of the sync raw-constructor default — opt-in semantics.
        $real = $this->createMock(PostgresExecutor::class);
        $cached = new CachedConnection($real, $this->cache);
        $this->assertFalse($cached->isAggressiveVerifyActive());
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
        $this->assertFalse($cached->isAggressiveVerifyActive());
    }

    public function testCachedConnectionAutoTriggersDetection(): void
    {
        // Auto mode + a detector that returns true → resolves to active.
        // We supply a fake-detection result by mocking the executor's
        // query() with the detection-SQL match.
        $detectionResult = $this->makeResult([['present' => true]]);
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_trigger'))
            ->willReturn($detectionResult);

        $cached = new CachedConnection(
            $real,
            $this->cache,
            AggressiveVerify::MODE_AUTO,
            'amp-test-' . __METHOD__,
        );
        $this->assertTrue($cached->isAggressiveVerifyActive());
    }

    public function testCachedConnectionAutoFalseDetection(): void
    {
        $detectionResult = $this->makeResult([['present' => false]]);
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_trigger'))
            ->willReturn($detectionResult);

        $cached = new CachedConnection(
            $real,
            $this->cache,
            AggressiveVerify::MODE_AUTO,
            'amp-test-' . __METHOD__,
        );
        $this->assertFalse($cached->isAggressiveVerifyActive());
    }

    public function testCachedConnectionLicensePayloadOverridesDetection(): void
    {
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=true');
        $real = $this->createMock(PostgresExecutor::class);
        // No query() call — license payload bypasses detection.
        $real->expects($this->never())->method('query');
        $cached = new CachedConnection(
            $real,
            $this->cache,
            AggressiveVerify::MODE_AUTO,
            'amp-test-' . __METHOD__,
        );
        $this->assertTrue($cached->isAggressiveVerifyActive());
    }

    public function testPostWriteVerifyAsyncIsNoopWhenInactive(): void
    {
        // Off mode → postWriteVerifyAsync is an early return, never
        // schedules an async probe. We assert by checking that no extra
        // query() calls fire on the executor.
        $real = $this->createMock(PostgresExecutor::class);
        $real->expects($this->never())->method('query');
        $cached = new CachedConnection($real, $this->cache, AggressiveVerify::MODE_OFF);
        $cached->postWriteVerifyAsync('INSERT INTO orders VALUES (1)');
    }
}
