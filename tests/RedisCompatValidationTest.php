<?php

namespace GoldLapel\Tests;

use GoldLapel\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Regression: Redis-compat helpers must reject injection-shaped identifier args.
 * See v0.2 security review finding C1.
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

    public function testEnqueueRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::enqueue($this->pdo(), self::BAD, []);
    }

    public function testDequeueRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::dequeue($this->pdo(), self::BAD);
    }

    public function testIncrRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::incr($this->pdo(), self::BAD, 'k');
    }

    public function testGetCounterRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::getCounter($this->pdo(), self::BAD, 'k');
    }

    public function testZaddRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zadd($this->pdo(), self::BAD, 'm', 1.0);
    }

    public function testZincrbyRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zincrby($this->pdo(), self::BAD, 'm');
    }

    public function testZrangeRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zrange($this->pdo(), self::BAD);
    }

    public function testZrankRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zrank($this->pdo(), self::BAD, 'm');
    }

    public function testZscoreRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zscore($this->pdo(), self::BAD, 'm');
    }

    public function testZremRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::zrem($this->pdo(), self::BAD, 'm');
    }

    public function testHsetRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hset($this->pdo(), self::BAD, 'k', 'f', 'v');
    }

    public function testHgetRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hget($this->pdo(), self::BAD, 'k', 'f');
    }

    public function testHgetallRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hgetall($this->pdo(), self::BAD, 'k');
    }

    public function testHdelRejectsBad(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::hdel($this->pdo(), self::BAD, 'k', 'f');
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

    public function testGeoaddRejectsBadTable(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geoadd($this->pdo(), self::BAD, 'name', 'geom', 'x', 0.0, 0.0);
    }

    public function testGeoaddRejectsBadNameColumn(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geoadd($this->pdo(), 'tbl', self::BAD, 'geom', 'x', 0.0, 0.0);
    }

    public function testGeoaddRejectsBadGeomColumn(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geoadd($this->pdo(), 'tbl', 'name', self::BAD, 'x', 0.0, 0.0);
    }

    public function testGeoradiusRejectsBadTable(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::georadius($this->pdo(), self::BAD, 'geom', 0.0, 0.0, 100.0);
    }

    public function testGeoradiusRejectsBadGeomColumn(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::georadius($this->pdo(), 'tbl', self::BAD, 0.0, 0.0, 100.0);
    }

    public function testGeodistRejectsBadTable(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::geodist($this->pdo(), self::BAD, 'geom', 'name', 'a', 'b');
    }

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
