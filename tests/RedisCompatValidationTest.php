<?php

namespace GoldLapel\Tests;

use GoldLapel\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Regression: identifier-style helpers must reject injection-shaped name
 * args. See v0.2 security review finding C1.
 *
 * After Phase 5 of schema-to-core, the legacy flat helpers (`incr`,
 * `hset`, `zadd`, `enqueue`, `geoadd`, …) are gone — the proxy owns DDL
 * and the wrapper dispatches via `Utils::counter*`, `Utils::zset*`, etc.
 * Identifier-validation parity is preserved across the new helpers below.
 */
#[AllowMockObjectsWithoutExpectations]
class RedisCompatValidationTest extends TestCase
{
    private const BAD = 'foo; DROP TABLE users--';

    private function pdo(): \PDO
    {
        return $this->createMock(\PDO::class);
    }

    public function testPublishRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::publish($this->pdo(), self::BAD, 'm');
    }

    public function testSubscribeRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::subscribe($this->pdo(), self::BAD, function () {});
    }

    public function testCountDistinctRejectsBadTable(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::countDistinct($this->pdo(), self::BAD, 'col');
    }

    public function testCountDistinctRejectsBadColumn(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::countDistinct($this->pdo(), 'tbl', self::BAD);
    }

    // ---- Phase 5 family helpers ----

    public function testCounterIncrRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::counterIncr($this->pdo(), self::BAD, 'k');
    }

    public function testCounterGetRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::counterGet($this->pdo(), self::BAD, 'k');
    }

    public function testZsetAddRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zsetAdd($this->pdo(), self::BAD, 'k', 'm', 1.0);
    }

    public function testZsetIncrByRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zsetIncrBy($this->pdo(), self::BAD, 'k', 'm');
    }

    public function testZsetRangeRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zsetRange($this->pdo(), self::BAD, 'k');
    }

    public function testZsetRankRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zsetRank($this->pdo(), self::BAD, 'k', 'm');
    }

    public function testZsetScoreRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zsetScore($this->pdo(), self::BAD, 'k', 'm');
    }

    public function testZsetRemoveRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zsetRemove($this->pdo(), self::BAD, 'k', 'm');
    }

    public function testHashSetRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hashSet($this->pdo(), self::BAD, 'k', 'f', 'v');
    }

    public function testHashGetRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hashGet($this->pdo(), self::BAD, 'k', 'f');
    }

    public function testHashGetAllRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hashGetAll($this->pdo(), self::BAD, 'k');
    }

    public function testHashDeleteRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hashDelete($this->pdo(), self::BAD, 'k', 'f');
    }

    public function testQueueEnqueueRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::queueEnqueue($this->pdo(), self::BAD, []);
    }

    public function testQueueClaimRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::queueClaim($this->pdo(), self::BAD);
    }

    public function testQueueAckRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::queueAck($this->pdo(), self::BAD, 1);
    }

    public function testGeoAddRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geoAdd($this->pdo(), self::BAD, 'm', 0.0, 0.0);
    }

    public function testGeoRadiusRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geoRadius($this->pdo(), self::BAD, 0.0, 0.0, 100.0);
    }

    public function testGeoDistRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geoDist($this->pdo(), self::BAD, 'a', 'b');
    }

    // ---- Stream helpers (kept from Phase 4) ----

    public function testStreamAddRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::streamAdd($this->pdo(), self::BAD, []);
    }

    public function testStreamCreateGroupRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::streamCreateGroup($this->pdo(), self::BAD, 'g');
    }

    public function testStreamReadRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::streamRead($this->pdo(), self::BAD, 'g', 'c');
    }

    public function testStreamAckRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::streamAck($this->pdo(), self::BAD, 'g', 1);
    }

    public function testStreamClaimRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::streamClaim($this->pdo(), self::BAD, 'g', 'c');
    }
}
