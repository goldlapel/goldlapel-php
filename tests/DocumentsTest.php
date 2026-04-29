<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\Documents;
use GoldLapel\GoldLapel;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Documents — the nested `$gl->documents` namespace
 * introduced in Phase 4 of schema-to-core.
 *
 * These tests verify:
 *   - $gl->documents is a Documents bound to the parent client
 *   - Each verb fetches DDL patterns from the proxy (or from the cache) then
 *     dispatches to Utils
 *   - The cache is shared with the parent client (one HTTP call per
 *     (family, name) per session)
 *   - $lookup.from collections in aggregate are resolved via the proxy too
 *   - The flat `docInsert` / `docFind` / etc. methods are gone (hard cut)
 *
 * Mirrors goldlapel-python/tests/test_documents.py.
 */
#[AllowMockObjectsWithoutExpectations]
class DocumentsTest extends TestCase
{
    private function makeGl(): GoldLapel
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        // Inject a mock PDO so resolveConn() doesn't throw.
        $pdo = $this->createMock(\PDO::class);
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        $ref->setValue($gl, $pdo);
        return $gl;
    }

    private function seedCache(GoldLapel $gl, string $family, string $name, string $table): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'ddlCache');
        $ref->setAccessible(true);
        $cache = $ref->getValue($gl);
        $cache["{$family}:{$name}"] = [
            'tables' => ['main' => $table],
            'query_patterns' => [],
        ];
        $ref->setValue($gl, $cache);
    }

    // ---- Namespace shape ----

    public function testDocumentsIsADocumentsInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Documents::class, $gl->documents);
    }

    public function testDocumentsIsReadonlyProperty(): void
    {
        // PHP 8.1 readonly: assigning to $gl->documents must throw.
        $gl = $this->makeGl();
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->documents = new Documents($gl);
    }

    public function testNoLegacyFlatDocMethodsOnGl(): void
    {
        // Hard cut — the flat doc* methods are gone. New code must use
        // $gl->documents-><verb>(...).
        $legacy = [
            'docInsert', 'docInsertMany', 'docFind', 'docFindOne', 'docFindCursor',
            'docUpdate', 'docUpdateOne', 'docDelete', 'docDeleteOne',
            'docFindOneAndUpdate', 'docFindOneAndDelete', 'docDistinct',
            'docCount', 'docCreateIndex', 'docAggregate', 'docWatch', 'docUnwatch',
            'docCreateTtlIndex', 'docRemoveTtlIndex', 'docCreateCollection',
            'docCreateCapped', 'docRemoveCap',
        ];
        foreach ($legacy as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Legacy flat method GoldLapel::{$name}() should have been removed; "
                . "use \$gl->documents-><verb>() instead."
            );
        }
    }

    // ---- Verb dispatch ----

    public function testInsertDispatchesWithCanonicalTable(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', '_goldlapel.doc_users');

        $row = ['_id' => 'u1', 'data' => '{"name":"alice"}', 'created_at' => '2026-01-01'];
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn($row);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('INSERT INTO _goldlapel.doc_users', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = $gl->documents->insert('users', ['name' => 'alice']);
        $this->assertSame('u1', $result['_id']);
    }

    public function testFindDispatchesWithFilterAndCanonicalTable(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', '_goldlapel.doc_users');

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('FROM _goldlapel.doc_users', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $gl->documents->find('users', ['active' => true]);
    }

    public function testCountDispatchesWithFilterAndCanonicalTable(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', '_goldlapel.doc_users');

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(42);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $count = $gl->documents->count('users', ['active' => true]);
        $this->assertSame(42, $count);
    }

    public function testUpdateOnePassesFilterAndUpdate(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', '_goldlapel.doc_users');

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('UPDATE _goldlapel.doc_users', $sql);
                $this->assertStringContainsString('WITH target AS', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $count = $gl->documents->updateOne('users', ['id' => 1], ['$set' => ['name' => 'x']]);
        $this->assertSame(1, $count);
    }

    public function testCreateCollectionFetchesPatternsButIssuesNoSql(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', '_goldlapel.doc_users');

        $pdo = $this->getInternalPdo($gl);
        // The proxy already created the table — no SQL on the user PDO.
        $pdo->expects($this->never())->method('exec');
        $pdo->expects($this->never())->method('prepare');

        $gl->documents->createCollection('users');
    }

    // ---- Aggregate $lookup resolution ----

    public function testAggregateResolvesLookupFromCollections(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', '_goldlapel.doc_users');
        $this->seedCache($gl, 'doc_store', 'orders', '_goldlapel.doc_orders');

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                // Source FROM clause uses the user-collection's canonical table.
                $this->assertStringContainsString('FROM _goldlapel.doc_users', $sql);
                // The $lookup subquery uses the resolved canonical table for `from`.
                $this->assertStringContainsString(
                    "FROM _goldlapel.doc_orders _lk",
                    $sql
                );
                return true;
            }))
            ->willReturn($stmt);

        $gl->documents->aggregate('users', [
            ['$match' => ['active' => true]],
            ['$lookup' => [
                'from' => 'orders',
                'localField' => 'id',
                'foreignField' => 'userId',
                'as' => 'user_orders',
            ]],
        ]);
    }

    // ---- Pattern cache sharing ----

    public function testPatternsAreCachedOnTheParentClient(): void
    {
        // The cache is keyed by (family, name). After the first call, no further
        // HTTP fetch — verify by seeding the cache with a sentinel and observing
        // it's returned unchanged on subsequent calls.
        $gl = $this->makeGl();
        $this->seedCache($gl, 'doc_store', 'users', 'sentinel.users');

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(0);
        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')
            ->with($this->callback(function (string $sql) {
                // The sentinel table name proves the cache hit (no HTTP call
                // would have rewritten the value).
                $this->assertStringContainsString('FROM sentinel.users', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $gl->documents->count('users');
        $gl->documents->count('users');
        $gl->documents->count('users');
    }

    // ---- Helpers ----

    private function getInternalPdo(GoldLapel $gl): \PDO
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }
}
