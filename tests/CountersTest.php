<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\Counters;
use GoldLapel\GoldLapel;
use GoldLapel\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Counters — the nested `$gl->counters` namespace
 * introduced in Phase 5 of schema-to-core (counter / zset / hash / queue /
 * geo).
 *
 * Tests cover:
 *   - $gl->counters is a Counters bound to the parent client.
 *   - Each verb fetches DDL patterns from the proxy then dispatches to the
 *     `Utils::counter*` static helper with the right args.
 *   - The pattern cache is shared with the parent client (one HTTP call per
 *     (family, name) per session).
 *   - SQL builders use the proxy's canonical query patterns (no in-wrapper
 *     CREATE TABLE leaks).
 *   - Phase-5 counter `updated_at` parity: the canonical patterns reference
 *     `NOW()` on every UPDATE — wrappers don't paper over this.
 *
 * Mirrors goldlapel-python/tests/test_counters.py.
 */
#[AllowMockObjectsWithoutExpectations]
class CountersTest extends TestCase
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
        $main = '_goldlapel.counter_pageviews';
        return [
            'tables' => ['main' => $main],
            'query_patterns' => [
                'incr' => "INSERT INTO {$main} (key, value, updated_at) VALUES (\$1, \$2, NOW()) ON CONFLICT (key) DO UPDATE SET value = {$main}.value + EXCLUDED.value, updated_at = NOW() RETURNING value",
                'set' => "INSERT INTO {$main} (key, value, updated_at) VALUES (\$1, \$2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value",
                'get' => "SELECT value FROM {$main} WHERE key = \$1",
                'delete' => "DELETE FROM {$main} WHERE key = \$1",
                'count_keys' => "SELECT COUNT(*) FROM {$main}",
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

    public function testCountersIsACountersInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Counters::class, $gl->counters);
    }

    public function testCountersIsReadonlyProperty(): void
    {
        $gl = $this->makeGl();
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->counters = new Counters($gl);
    }

    public function testNoLegacyFlatCounterMethods(): void
    {
        $gl = $this->makeGl();
        foreach (['incr', 'getCounter'] as $name) {
            $this->assertFalse(
                method_exists($gl, $name),
                "Phase 5 removed flat {$name} — use \$gl->counters-><verb>()."
            );
        }
    }

    // ---- Verb dispatch ----

    public function testIncrUsesCanonicalTableAndBindsKeyAmount(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'counter', 'pageviews', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(7);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('INSERT INTO _goldlapel.counter_pageviews', $sql);
                $this->assertStringContainsString('updated_at = NOW()', $sql);
                $this->assertStringNotContainsString('$', $sql); // $N → ?
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with(['home', 5]);

        $result = $gl->counters->incr('pageviews', 'home', 5);
        $this->assertSame(7, $result);
    }

    public function testDecrPassesNegativeAmount(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'counter', 'pageviews', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchColumn')->willReturn(-2);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['home', -3]);

        $result = $gl->counters->decr('pageviews', 'home', 3);
        $this->assertSame(-2, $result);
    }

    public function testSetUsesSetPattern(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'counter', 'pageviews', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchColumn')->willReturn(100);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                // The `set` pattern uses EXCLUDED.value (NOT incrementing).
                $this->assertStringContainsString('value = EXCLUDED.value', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $this->assertSame(100, $gl->counters->set('pageviews', 'home', 100));
    }

    public function testGetReturnsZeroForUnknownKey(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'counter', 'pageviews', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(false); // PDO empty result

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(0, $gl->counters->get('pageviews', 'missing'));
    }

    public function testDeleteReturnsTrueOnRowcount(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'counter', 'pageviews', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertTrue($gl->counters->delete('pageviews', 'home'));
    }

    public function testCountKeysReturnsValue(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'counter', 'pageviews', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(5);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(5, $gl->counters->countKeys('pageviews'));
    }

    // ---- Pattern cache sharing ----

    public function testPatternsCachedOnParentClient(): void
    {
        $gl = $this->makeGl();
        $sentinel = $this->fakePatterns();
        $sentinel['tables']['main'] = 'sentinel.counters';
        $sentinel['query_patterns']['count_keys'] = 'SELECT COUNT(*) FROM sentinel.counters';
        $this->seedCache($gl, 'counter', 'pageviews', $sentinel);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(0);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('FROM sentinel.counters', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $gl->counters->countKeys('pageviews');
        $gl->counters->countKeys('pageviews');
        $gl->counters->countKeys('pageviews');
    }

    // ---- Phase 5 contract: updated_at on every UPDATE ----

    public function testCanonicalIncrPatternStampsUpdatedAt(): void
    {
        $patterns = $this->fakePatterns();
        $this->assertStringContainsString('updated_at = NOW()', $patterns['query_patterns']['incr']);
    }

    // ---- Direct-helper tests (Utils-level) ----

    public function testUtilsCounterIncrTranslatesPlaceholders(): void
    {
        $patterns = $this->fakePatterns();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchColumn')->willReturn(5);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                // After $N → ? translation, the pattern has two `?` markers.
                $this->assertSame(2, substr_count($sql, '?'));
                return true;
            }))
            ->willReturn($stmt);

        $this->assertSame(5, Utils::counterIncr($pdo, 'pageviews', 'home', 5, $patterns));
    }

    public function testUtilsRequiresPatternsWithActionableMessage(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('counter');
        Utils::counterIncr($pdo, 'pageviews', 'home', 1, null);
    }
}
