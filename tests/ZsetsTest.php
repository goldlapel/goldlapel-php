<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Utils;
use GoldLapel\Zsets;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Zsets — the nested `$gl->zsets` namespace.
 *
 * Phase 5 introduced a `zset_key` column in the canonical schema so a
 * single namespace table holds many sorted sets. These tests verify:
 *   - `$zsetKey` threads through every method as the first positional arg
 *     after the namespace `$name` (matching Redis ZADD signatures).
 *   - Pattern selection picks `zrange_asc` vs `zrange_desc` based on the
 *     `$desc` arg.
 *   - Range/limit translation is Redis-inclusive (start..stop inclusive).
 *   - SQL builders bind in `(zsetKey, member, score)` order matching the
 *     proxy's `$1, $2, $3` template.
 *
 * Mirrors goldlapel-python/tests/test_zsets.py.
 */
#[AllowMockObjectsWithoutExpectations]
class ZsetsTest extends TestCase
{
    private function makeGl(): GoldLapel
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $pdo = $this->createMock(\PDO::class);
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        $ref->setValue($gl, $pdo);
        return $gl;
    }

    private function fakePatterns(): array
    {
        $main = '_goldlapel.zset_leaderboard';
        return [
            'tables' => ['main' => $main],
            'query_patterns' => [
                'zadd' => "INSERT INTO {$main} (zset_key, member, score) VALUES (\$1, \$2, \$3) ON CONFLICT (zset_key, member) DO UPDATE SET score = EXCLUDED.score RETURNING score",
                'zincrby' => "INSERT INTO {$main} (zset_key, member, score) VALUES (\$1, \$2, \$3) ON CONFLICT (zset_key, member) DO UPDATE SET score = {$main}.score + EXCLUDED.score RETURNING score",
                'zscore' => "SELECT score FROM {$main} WHERE zset_key = \$1 AND member = \$2",
                'zrem' => "DELETE FROM {$main} WHERE zset_key = \$1 AND member = \$2",
                'zrange_asc' => "SELECT member, score FROM {$main} WHERE zset_key = \$1 ORDER BY score ASC, member ASC LIMIT \$2 OFFSET \$3",
                'zrange_desc' => "SELECT member, score FROM {$main} WHERE zset_key = \$1 ORDER BY score DESC, member DESC LIMIT \$2 OFFSET \$3",
                'zrangebyscore' => "SELECT member, score FROM {$main} WHERE zset_key = \$1 AND score >= \$2 AND score <= \$3 ORDER BY score ASC, member ASC LIMIT \$4 OFFSET \$5",
                'zrank_asc' => "SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rank FROM {$main} WHERE zset_key = \$1 ) ranked WHERE member = \$2",
                'zrank_desc' => "SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score DESC, member DESC) - 1 AS rank FROM {$main} WHERE zset_key = \$1 ) ranked WHERE member = \$2",
                'zcard' => "SELECT COUNT(*) FROM {$main} WHERE zset_key = \$1",
            ],
        ];
    }

    private function seedCache(GoldLapel $gl, string $family, string $name, array $patterns): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'ddlCache');
        $ref->setAccessible(true);
        $cache = $ref->getValue($gl);
        $cache["{$family}:{$name}"] = $patterns;
        $ref->setValue($gl, $cache);
    }

    private function getInternalPdo(GoldLapel $gl): \PDO
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    // ---- Namespace shape ----

    public function testZsetsIsAZsetsInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Zsets::class, $gl->zsets);
    }

    public function testNoLegacyFlatZsetMethods(): void
    {
        $gl = $this->makeGl();
        foreach (['zadd', 'zincrby', 'zrange', 'zrank', 'zscore', 'zrem'] as $name) {
            $this->assertFalse(method_exists($gl, $name));
        }
    }

    // ---- Verb dispatch & binding order ----

    public function testAddBindsZsetKeyMemberScoreInOrder(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchColumn')->willReturn(100.0);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['global', 'alice', 100.0]);

        $this->assertSame(100.0, $gl->zsets->add('leaderboard', 'global', 'alice', 100));
    }

    public function testIncrByPassesDelta(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchColumn')->willReturn(110.0);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['global', 'alice', 10.0]);

        $this->assertSame(110.0, $gl->zsets->incrBy('leaderboard', 'global', 'alice', 10));
    }

    public function testRangePicksDescPattern(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([['alice', 100.0], ['bob', 90.0]]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('ORDER BY score DESC', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $rows = $gl->zsets->range('leaderboard', 'global', 0, 1, true);
        $this->assertSame([['alice', 100.0], ['bob', 90.0]], $rows);
    }

    public function testRangePicksAscPattern(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('ORDER BY score ASC', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $gl->zsets->range('leaderboard', 'global', 0, 5, false);
    }

    public function testRangeTranslatesInclusiveStopToLimit(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        // Inclusive 0..9 → LIMIT 10 OFFSET 0
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['global', 10, 0]);

        $gl->zsets->range('leaderboard', 'global', 0, 9);
    }

    public function testRangeStopMinusOneMapsToLargeLimit(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['global', 10000, 0]);

        $gl->zsets->range('leaderboard', 'global');
    }

    public function testRangeByScoreInclusiveBounds(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['global', 50.0, 200.0, 10, 2]);

        $gl->zsets->rangeByScore('leaderboard', 'global', 50, 200, 10, 2);
    }

    public function testRemoveReturnsBoolFromRowcount(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertTrue($gl->zsets->remove('leaderboard', 'global', 'alice'));
    }

    public function testCardReturnsZeroForUnknownKey(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'zset', 'leaderboard', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(false);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(0, $gl->zsets->card('leaderboard', 'missing'));
    }

    // ---- Direct-helper tests ----

    public function testUtilsZsetAddBindsZsetKeyMemberScore(): void
    {
        $patterns = $this->fakePatterns();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchColumn')->willReturn(100.0);

        $pdo = $this->createMock(\PDO::class);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['global', 'alice', 100.0]);

        Utils::zsetAdd($pdo, 'leaderboard', 'global', 'alice', 100, $patterns);
    }
}
