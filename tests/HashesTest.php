<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Hashes;
use GoldLapel\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Hashes — the nested `$gl->hashes` namespace.
 *
 * Phase 5 flipped the hash storage shape from "JSONB blob per key" to
 * "row per (hash_key, field)". These tests verify:
 *   - The wrapper executes single-row UPSERT for `set`, NOT load-merge-save.
 *   - `getAll` aggregates rows from the proxy into a PHP assoc array.
 *   - `keys` / `values` return per-row sequences (not blob extraction).
 *   - `delete` returns true/false from rowcount, not from JSONB key probe.
 *
 * Mirrors goldlapel-python/tests/test_hashes.py.
 */
#[AllowMockObjectsWithoutExpectations]
class HashesTest extends TestCase
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
        $main = '_goldlapel.hash_sessions';
        return [
            'tables' => ['main' => $main],
            'query_patterns' => [
                'hset' => "INSERT INTO {$main} (hash_key, field, value) VALUES (\$1, \$2, \$3::jsonb) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value RETURNING value",
                'hget' => "SELECT value FROM {$main} WHERE hash_key = \$1 AND field = \$2",
                'hgetall' => "SELECT field, value FROM {$main} WHERE hash_key = \$1 ORDER BY field",
                'hkeys' => "SELECT field FROM {$main} WHERE hash_key = \$1 ORDER BY field",
                'hvals' => "SELECT value FROM {$main} WHERE hash_key = \$1 ORDER BY field",
                'hexists' => "SELECT EXISTS (SELECT 1 FROM {$main} WHERE hash_key = \$1 AND field = \$2)",
                'hdel' => "DELETE FROM {$main} WHERE hash_key = \$1 AND field = \$2",
                'hlen' => "SELECT COUNT(*) FROM {$main} WHERE hash_key = \$1",
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

    public function testHashesIsAHashesInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Hashes::class, $gl->hashes);
    }

    public function testNoLegacyFlatHashMethods(): void
    {
        $gl = $this->makeGl();
        foreach (['hset', 'hget', 'hgetall', 'hdel'] as $name) {
            $this->assertFalse(method_exists($gl, $name));
        }
    }

    // ---- Verb dispatch ----

    public function testSetIsSingleRowUpsertNotLoadMerge(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(['"alice"']);

        $pdo = $this->getInternalPdo($gl);
        // Single prepare — no SELECT-then-merge-then-update sequence.
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('INSERT INTO _goldlapel.hash_sessions', $sql);
                $this->assertStringContainsString('ON CONFLICT (hash_key, field)', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $gl->hashes->set('sessions', 'user:1', 'name', 'alice');
    }

    public function testSetJsonEncodesValueAndBindsInOrder(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(['{"a":1}']);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['user:1', 'data', '{"a":1}']);

        $gl->hashes->set('sessions', 'user:1', 'data', ['a' => 1]);
    }

    public function testGetAllRebuildsAssocArrayFromRows(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        // Simulate two rows from PDOStatement::fetch(\PDO::FETCH_NUM).
        $stmt->method('fetch')->willReturnOnConsecutiveCalls(
            ['email', '"a@x"'],
            ['name', '"alice"'],
            false,
        );

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(
            ['email' => 'a@x', 'name' => 'alice'],
            $gl->hashes->getAll('sessions', 'user:1'),
        );
    }

    public function testGetAllDecodesJsonbString(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturnOnConsecutiveCalls(
            ['data', '{"k": 1}'],
            false,
        );

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(
            ['data' => ['k' => 1]],
            $gl->hashes->getAll('sessions', 'user:1'),
        );
    }

    public function testGetReturnsNullForAbsentField(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertNull($gl->hashes->get('sessions', 'user:1', 'missing'));
    }

    public function testKeysReturnsFieldList(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([['name'], ['email']]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(
            ['name', 'email'],
            $gl->hashes->keys('sessions', 'user:1'),
        );
    }

    public function testExistsReturnsBool(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(true);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertTrue($gl->hashes->exists('sessions', 'user:1', 'name'));
    }

    public function testDeleteReturnsTrueWhenRemoved(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertTrue($gl->hashes->delete('sessions', 'user:1', 'name'));
    }

    public function testLenReturnsCount(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'hash', 'sessions', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(3);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(3, $gl->hashes->len('sessions', 'user:1'));
    }

    // ---- Phase 5 contract: row-per-field schema, not JSONB blob ----

    public function testCanonicalPatternIsRowPerFieldNotBlob(): void
    {
        $sql = $this->fakePatterns()['query_patterns']['hset'];
        $this->assertStringContainsString('(hash_key, field, value)', $sql);
        $this->assertStringNotContainsString('jsonb_build_object', $sql);
    }
}
