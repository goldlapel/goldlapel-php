<?php

namespace GoldLapel\Tests;

use GoldLapel\Utils;
use PHPUnit\Framework\TestCase;

class DocTest extends TestCase
{
    private function makeMockPDO(): \PDO
    {
        return $this->createMock(\PDO::class);
    }

    private function makeMockStmt(array $rows = [], int $rowCount = 0): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn($rows);
        $stmt->method('fetch')->willReturn($rows[0] ?? false);
        $stmt->method('fetchColumn')->willReturn($rows[0]['count'] ?? false);
        $stmt->method('rowCount')->willReturn($rowCount);
        return $stmt;
    }

    // ========================================================================
    // docInsert
    // ========================================================================

    public function testDocInsertEnsuresCollectionAndInserts(): void
    {
        $row = ['_id' => 'abc-123', 'data' => '{"name":"Alice"}', 'created_at' => '2026-01-01'];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([$row]);

        $pdo->expects($this->once())->method('exec')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS users', $sql);
                $this->assertStringContainsString('_id UUID PRIMARY KEY', $sql);
                $this->assertStringContainsString('data JSONB NOT NULL', $sql);
                $this->assertStringContainsString('created_at TIMESTAMPTZ', $sql);
                return true;
            }));

        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('INSERT INTO users', $sql);
                $this->assertStringContainsString('?::jsonb', $sql);
                $this->assertStringContainsString('RETURNING _id, data, created_at', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['name' => 'Alice'])]);

        $result = Utils::docInsert($pdo, 'users', ['name' => 'Alice']);
        $this->assertSame($row, $result);
    }

    public function testDocInsertInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::docInsert($pdo, 'DROP TABLE x; --', ['a' => 1]);
    }

    // ========================================================================
    // docInsertMany
    // ========================================================================

    public function testDocInsertManyBatchInserts(): void
    {
        $rows = [
            ['_id' => 'a', 'data' => '{"x":1}', 'created_at' => 'now'],
            ['_id' => 'b', 'data' => '{"x":2}', 'created_at' => 'now'],
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('exec');
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('INSERT INTO items', $sql);
                $this->assertStringContainsString('(?::jsonb), (?::jsonb)', $sql);
                $this->assertStringContainsString('RETURNING _id, data, created_at', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['x' => 1]), json_encode(['x' => 2])]);

        $result = Utils::docInsertMany($pdo, 'items', [['x' => 1], ['x' => 2]]);
        $this->assertCount(2, $result);
    }

    public function testDocInsertManySingleDocument(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['_id' => 'a']]);
        $pdo->method('exec');
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('(?::jsonb)', $sql);
                $this->assertStringNotContainsString('(?::jsonb), (?::jsonb)', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docInsertMany($pdo, 'items', [['name' => 'solo']]);
    }

    public function testDocInsertManyInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docInsertMany($pdo, '1bad', [['a' => 1]]);
    }

    // ========================================================================
    // docFind
    // ========================================================================

    public function testDocFindWithFilter(): void
    {
        $rows = [['_id' => 'a', 'data' => '{"name":"Alice"}', 'created_at' => 'now']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SELECT _id, data, created_at FROM users', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['active' => true])]);

        $result = Utils::docFind($pdo, 'users', ['active' => true]);
        $this->assertCount(1, $result);
    }

    public function testDocFindWithoutFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SELECT _id, data, created_at FROM users', $sql);
                $this->assertStringNotContainsString('WHERE', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')->with([]);

        Utils::docFind($pdo, 'users');
    }

    public function testDocFindEmptyFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringNotContainsString('WHERE', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docFind($pdo, 'users', []);
    }

    public function testDocFindWithSortLimitSkip(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("ORDER BY data->>'name' ASC, data->>'age' DESC", $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                $this->assertStringContainsString('OFFSET ?', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['active' => true]), 10, 20]);

        Utils::docFind($pdo, 'users', ['active' => true], ['name' => 1, 'age' => -1], 10, 20);
    }

    public function testDocFindLimitSkipWithoutFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringNotContainsString('WHERE', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                $this->assertStringContainsString('OFFSET ?', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([5, 0]);

        Utils::docFind($pdo, 'users', null, null, 5, 0);
    }

    public function testDocFindInvalidSortKey(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid sort key');
        Utils::docFind($pdo, 'users', [], ['DROP;--' => 1]);
    }

    public function testDocFindInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docFind($pdo, 'bad table', []);
    }

    // ========================================================================
    // docFindOne
    // ========================================================================

    public function testDocFindOneWithFilter(): void
    {
        $row = ['_id' => 'a', 'data' => '{"name":"Bob"}', 'created_at' => 'now'];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([$row]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SELECT _id, data, created_at FROM users', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                $this->assertStringContainsString('LIMIT 1', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['name' => 'Bob'])]);

        $result = Utils::docFindOne($pdo, 'users', ['name' => 'Bob']);
        $this->assertSame($row, $result);
    }

    public function testDocFindOneReturnsNull(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);
        $pdo->method('prepare')->willReturn($stmt);

        $result = Utils::docFindOne($pdo, 'users', ['name' => 'Nobody']);
        $this->assertNull($result);
    }

    public function testDocFindOneEmptyFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringNotContainsString('WHERE', $sql);
                $this->assertStringContainsString('LIMIT 1', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docFindOne($pdo, 'users', []);
    }

    public function testDocFindOneInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docFindOne($pdo, 'bad;table', []);
    }

    // ========================================================================
    // docUpdate
    // ========================================================================

    public function testDocUpdateGeneratesCorrectSql(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([], 3);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('UPDATE users SET data = data || ?::jsonb', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['active' => true]), json_encode(['active' => false])]);

        $result = Utils::docUpdate($pdo, 'users', ['active' => false], ['active' => true]);
        $this->assertSame(3, $result);
    }

    public function testDocUpdateReturnsRowCount(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([], 0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = Utils::docUpdate($pdo, 'users', ['x' => 1], ['x' => 2]);
        $this->assertSame(0, $result);
    }

    public function testDocUpdateInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docUpdate($pdo, '1bad', [], []);
    }

    // ========================================================================
    // docUpdateOne
    // ========================================================================

    public function testDocUpdateOneUsesCte(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([], 1);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('WITH target AS', $sql);
                $this->assertStringContainsString('SELECT _id FROM users WHERE data @> ?::jsonb LIMIT 1', $sql);
                $this->assertStringContainsString('UPDATE users SET data = data || ?::jsonb', $sql);
                $this->assertStringContainsString('FROM target WHERE users._id = target._id', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['name' => 'Alice']), json_encode(['age' => 30])]);

        $result = Utils::docUpdateOne($pdo, 'users', ['name' => 'Alice'], ['age' => 30]);
        $this->assertSame(1, $result);
    }

    public function testDocUpdateOneInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docUpdateOne($pdo, 'DROP; --', [], []);
    }

    // ========================================================================
    // docDelete
    // ========================================================================

    public function testDocDeleteGeneratesCorrectSql(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([], 5);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('DELETE FROM users', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['archived' => true])]);

        $result = Utils::docDelete($pdo, 'users', ['archived' => true]);
        $this->assertSame(5, $result);
    }

    public function testDocDeleteReturnsRowCount(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([], 0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = Utils::docDelete($pdo, 'users', ['x' => 1]);
        $this->assertSame(0, $result);
    }

    public function testDocDeleteInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docDelete($pdo, 'bad table', []);
    }

    // ========================================================================
    // docDeleteOne
    // ========================================================================

    public function testDocDeleteOneUsesCte(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([], 1);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('WITH target AS', $sql);
                $this->assertStringContainsString('SELECT _id FROM users WHERE data @> ?::jsonb LIMIT 1', $sql);
                $this->assertStringContainsString('DELETE FROM users USING target', $sql);
                $this->assertStringContainsString('WHERE users._id = target._id', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['name' => 'Alice'])]);

        $result = Utils::docDeleteOne($pdo, 'users', ['name' => 'Alice']);
        $this->assertSame(1, $result);
    }

    public function testDocDeleteOneInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docDeleteOne($pdo, '1no', []);
    }

    // ========================================================================
    // docCount
    // ========================================================================

    public function testDocCountWithFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(42);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SELECT COUNT(*) FROM users', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['active' => true])]);

        $result = Utils::docCount($pdo, 'users', ['active' => true]);
        $this->assertSame(42, $result);
    }

    public function testDocCountWithoutFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(100);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SELECT COUNT(*) FROM users', $sql);
                $this->assertStringNotContainsString('WHERE', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')->with([]);

        $result = Utils::docCount($pdo, 'users');
        $this->assertSame(100, $result);
    }

    public function testDocCountEmptyFilter(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(0);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringNotContainsString('WHERE', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docCount($pdo, 'users', []);
    }

    public function testDocCountReturnsInt(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn('999');
        $pdo->method('prepare')->willReturn($stmt);

        $result = Utils::docCount($pdo, 'items');
        $this->assertIsInt($result);
        $this->assertSame(999, $result);
    }

    public function testDocCountInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docCount($pdo, 'bad;name', []);
    }

    // ========================================================================
    // docCreateIndex
    // ========================================================================

    public function testDocCreateIndexGinWhenNoKeys(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->expects($this->once())->method('exec')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('CREATE INDEX IF NOT EXISTS users_data_gin', $sql);
                $this->assertStringContainsString('ON users USING GIN (data)', $sql);
                return true;
            }));

        Utils::docCreateIndex($pdo, 'users');
    }

    public function testDocCreateIndexGinForEmptyKeys(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->expects($this->once())->method('exec')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('USING GIN', $sql);
                return true;
            }));

        Utils::docCreateIndex($pdo, 'users', []);
    }

    public function testDocCreateIndexBtreePerKey(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docCreateIndex($pdo, 'users', ['name' => 1, 'age' => -1]);

        $this->assertCount(2, $execCalls);
        $this->assertStringContainsString('CREATE INDEX IF NOT EXISTS users_name_idx', $execCalls[0]);
        $this->assertStringContainsString("(data->>'name') ASC", $execCalls[0]);
        $this->assertStringContainsString('CREATE INDEX IF NOT EXISTS users_age_idx', $execCalls[1]);
        $this->assertStringContainsString("(data->>'age') DESC", $execCalls[1]);
    }

    public function testDocCreateIndexDottedKeyName(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docCreateIndex($pdo, 'events', ['user.email' => 1]);

        $this->assertCount(1, $execCalls);
        $this->assertStringContainsString('events_user_email_idx', $execCalls[0]);
        $this->assertStringContainsString("(data->>'user.email') ASC", $execCalls[0]);
    }

    public function testDocCreateIndexInvalidKey(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid index key');
        Utils::docCreateIndex($pdo, 'users', ['DROP;--' => 1]);
    }

    public function testDocCreateIndexInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docCreateIndex($pdo, 'bad table');
    }

    public function testDocCreateIndexReturnsVoid(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('exec');
        $result = Utils::docCreateIndex($pdo, 'users');
        $this->assertNull($result);
    }

    // ========================================================================
    // docAggregate
    // ========================================================================

    public function testDocAggregateFullPipeline(): void
    {
        $rows = [
            ['_id' => 'electronics', 'total' => '250'],
            ['_id' => 'books', 'total' => '100'],
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("data->>'category' AS _id", $sql);
                $this->assertStringContainsString("SUM((data->>'price')::numeric) AS total", $sql);
                $this->assertStringContainsString("WHERE data @> ?::jsonb", $sql);
                $this->assertStringContainsString("GROUP BY data->>'category'", $sql);
                $this->assertStringContainsString("ORDER BY total DESC", $sql);
                $this->assertStringContainsString("LIMIT ?", $sql);
                $this->assertStringContainsString("OFFSET ?", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['active' => true]), 10, 5]);

        $result = Utils::docAggregate($pdo, 'orders', [
            ['$match' => ['active' => true]],
            ['$group' => [
                '_id' => '$category',
                'total' => ['$sum' => '$price'],
            ]],
            ['$sort' => ['total' => -1]],
            ['$limit' => 10],
            ['$skip' => 5],
        ]);
        $this->assertCount(2, $result);
        $this->assertSame('electronics', $result[0]['_id']);
    }

    public function testDocAggregateAvgAccumulator(): void
    {
        $rows = [['_id' => 'A', 'avg_score' => '85.5']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("AVG((data->>'score')::numeric) AS avg_score", $sql);
                $this->assertStringContainsString("GROUP BY data->>'department'", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'employees', [
            ['$group' => [
                '_id' => '$department',
                'avg_score' => ['$avg' => '$score'],
            ]],
        ]);
        $this->assertCount(1, $result);
        $this->assertSame('85.5', $result[0]['avg_score']);
    }

    public function testDocAggregateNullGroupId(): void
    {
        $rows = [['total' => '500']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("SUM((data->>'amount')::numeric) AS total", $sql);
                $this->assertStringNotContainsString('GROUP BY', $sql);
                $this->assertStringNotContainsString('_id', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'orders', [
            ['$group' => [
                '_id' => null,
                'total' => ['$sum' => '$amount'],
            ]],
        ]);
        $this->assertCount(1, $result);
    }

    public function testDocAggregateMatchOnly(): void
    {
        $rows = [['_id' => 'a', 'data' => '{"status":"active"}', 'created_at' => 'now']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SELECT _id, data, created_at FROM orders', $sql);
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                $this->assertStringNotContainsString('GROUP BY', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['status' => 'active'])]);

        $result = Utils::docAggregate($pdo, 'orders', [
            ['$match' => ['status' => 'active']],
        ]);
        $this->assertCount(1, $result);
    }

    public function testDocAggregateSortContextAfterGroup(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('ORDER BY cnt DESC', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'events', [
            ['$group' => [
                '_id' => '$type',
                'cnt' => ['$count' => true],
            ]],
            ['$sort' => ['cnt' => -1]],
        ]);
    }

    public function testDocAggregateSortContextWithoutGroup(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("ORDER BY data->>'name' ASC", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'users', [
            ['$sort' => ['name' => 1]],
        ]);
    }

    public function testDocAggregateUnsupportedStage(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported pipeline stage: $bucket');
        Utils::docAggregate($pdo, 'users', [
            ['$bucket' => ['groupBy' => '$price']],
        ]);
    }

    public function testDocAggregateUnsupportedAccumulator(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported accumulator: $first');
        Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => '$status',
                'top' => ['$first' => '$name'],
            ]],
        ]);
    }

    public function testDocAggregateEmptyPipeline(): void
    {
        $pdo = $this->makeMockPDO();
        $result = Utils::docAggregate($pdo, 'users', []);
        $this->assertSame([], $result);
    }

    public function testDocAggregateInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docAggregate($pdo, 'bad table', []);
    }

    public function testDocAggregateCountAccumulator(): void
    {
        $rows = [['_id' => 'active', 'cnt' => '42']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("COUNT(*) AS cnt", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => '$status',
                'cnt' => ['$count' => true],
            ]],
        ]);
        $this->assertCount(1, $result);
    }

    public function testDocAggregateMinMaxAccumulators(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['_id' => 'A', 'lo' => '10', 'hi' => '99']]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("MIN((data->>'price')::numeric) AS lo", $sql);
                $this->assertStringContainsString("MAX((data->>'price')::numeric) AS hi", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'products', [
            ['$group' => [
                '_id' => '$category',
                'lo' => ['$min' => '$price'],
                'hi' => ['$max' => '$price'],
            ]],
        ]);
    }

    // ========================================================================
    // Composite $group._id + $push/$addToSet
    // ========================================================================

    public function testDocAggregateCompositeIdBasic(): void
    {
        $rows = [['_id' => '{"region":"US","type":"premium"}', 'total' => '500']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("json_build_object('region', data->>'region', 'type', data->>'type') AS _id", $sql);
                $this->assertStringContainsString("SUM((data->>'amount')::numeric) AS total", $sql);
                $this->assertStringContainsString("GROUP BY data->>'region', data->>'type'", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'sales', [
            ['$group' => [
                '_id' => ['region' => '$region', 'type' => '$type'],
                'total' => ['$sum' => '$amount'],
            ]],
        ]);
        $this->assertCount(1, $result);
    }

    public function testDocAggregateCompositeIdDotNotation(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("json_build_object('city', data->'address'->>'city', 'zip', data->'address'->>'zip') AS _id", $sql);
                $this->assertStringContainsString("GROUP BY data->'address'->>'city', data->'address'->>'zip'", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => ['city' => '$address.city', 'zip' => '$address.zip'],
                'cnt' => ['$count' => true],
            ]],
        ]);
    }

    public function testDocAggregateCompositeIdInvalidAlias(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid _id alias');
        Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => ['bad alias!' => '$field'],
                'cnt' => ['$count' => true],
            ]],
        ]);
    }

    public function testDocAggregateCompositeIdInvalidRef(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid field reference in _id');
        Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => ['region' => 'not_a_ref'],
                'cnt' => ['$count' => true],
            ]],
        ]);
    }

    public function testDocAggregateCompositeIdEmptyArray(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty associative array');
        Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => [],
                'cnt' => ['$count' => true],
            ]],
        ]);
    }

    public function testDocAggregatePushAccumulator(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['_id' => 'engineering', 'names' => '{Alice,Bob}']]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("array_agg(data->>'name') AS names", $sql);
                $this->assertStringContainsString("GROUP BY data->>'department'", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'employees', [
            ['$group' => [
                '_id' => '$department',
                'names' => ['$push' => '$name'],
            ]],
        ]);
        $this->assertCount(1, $result);
    }

    public function testDocAggregateAddToSetAccumulator(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['_id' => 'engineering', 'tags' => '{go,rust}']]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("array_agg(DISTINCT data->>'tag') AS tags", $sql);
                $this->assertStringContainsString("GROUP BY data->>'department'", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'employees', [
            ['$group' => [
                '_id' => '$department',
                'tags' => ['$addToSet' => '$tag'],
            ]],
        ]);
        $this->assertCount(1, $result);
    }

    public function testDocAggregateBackwardCompatSingleId(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['_id' => 'active', 'cnt' => '5']]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("data->>'status' AS _id", $sql);
                $this->assertStringContainsString("COUNT(*) AS cnt", $sql);
                $this->assertStringContainsString("GROUP BY data->>'status'", $sql);
                $this->assertStringNotContainsString('json_build_object', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $result = Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => '$status',
                'cnt' => ['$count' => true],
            ]],
        ]);
        $this->assertCount(1, $result);
    }

    // ========================================================================
    // Filter operators (buildFilter / fieldPath)
    // ========================================================================

    private function callBuildFilter(?array $filter): array
    {
        $method = new \ReflectionMethod(Utils::class, 'buildFilter');
        return $method->invoke(null, $filter);
    }

    public function testFilterGtNumeric(): void
    {
        [$clause, $params] = $this->callBuildFilter(['age' => ['$gt' => 25]]);
        $this->assertStringContainsString('::numeric', $clause);
        $this->assertStringContainsString('>', $clause);
        $this->assertSame([25], $params);
    }

    public function testFilterLteString(): void
    {
        [$clause, $params] = $this->callBuildFilter(['name' => ['$lte' => 'M']]);
        $this->assertStringNotContainsString('::numeric', $clause);
        $this->assertStringContainsString('<=', $clause);
        $this->assertSame(['M'], $params);
    }

    public function testFilterIn(): void
    {
        [$clause, $params] = $this->callBuildFilter(['status' => ['$in' => ['a', 'b']]]);
        $this->assertStringContainsString('IN (?, ?)', $clause);
        $this->assertSame(['a', 'b'], $params);
    }

    public function testFilterNin(): void
    {
        [$clause, $params] = $this->callBuildFilter(['status' => ['$nin' => ['x']]]);
        $this->assertStringContainsString('NOT IN (?)', $clause);
        $this->assertSame(['x'], $params);
    }

    public function testFilterExistsTrue(): void
    {
        [$clause, $params] = $this->callBuildFilter(['email' => ['$exists' => true]]);
        $this->assertStringContainsString('data ? ?', $clause);
        $this->assertStringNotContainsString('NOT', $clause);
        $this->assertSame(['email'], $params);
    }

    public function testFilterExistsFalse(): void
    {
        [$clause, $params] = $this->callBuildFilter(['email' => ['$exists' => false]]);
        $this->assertStringContainsString('NOT (data ? ?)', $clause);
        $this->assertSame(['email'], $params);
    }

    public function testFilterRegex(): void
    {
        [$clause, $params] = $this->callBuildFilter(['name' => ['$regex' => '^J']]);
        $this->assertStringContainsString('~ ?', $clause);
        $this->assertSame(['^J'], $params);
    }

    public function testFilterEqNe(): void
    {
        [$clause, $params] = $this->callBuildFilter(['x' => ['$eq' => 'a'], 'y' => ['$ne' => 'b']]);
        $this->assertStringContainsString('= ?', $clause);
        $this->assertStringContainsString('!= ?', $clause);
        $this->assertContains('a', $params);
        $this->assertContains('b', $params);
    }

    public function testFilterMixed(): void
    {
        [$clause, $params] = $this->callBuildFilter(['active' => true, 'age' => ['$gt' => 18]]);
        $this->assertStringContainsString('data @> ?::jsonb', $clause);
        $this->assertStringContainsString('::numeric >', $clause);
        $this->assertSame(json_encode(['active' => true]), $params[0]);
        $this->assertSame(18, $params[1]);
    }

    public function testFilterDotNotation(): void
    {
        [$clause, $params] = $this->callBuildFilter(['addr.city' => ['$eq' => 'NY']]);
        $this->assertStringContainsString("data->'addr'->>'city'", $clause);
        $this->assertSame(['NY'], $params);
    }

    public function testFilterRange(): void
    {
        [$clause, $params] = $this->callBuildFilter(['age' => ['$gte' => 18, '$lt' => 65]]);
        $this->assertStringContainsString('>=', $clause);
        $this->assertStringContainsString('<', $clause);
        $this->assertContains(18, $params);
        $this->assertContains(65, $params);
    }

    public function testFilterPlainUnchanged(): void
    {
        [$clause, $params] = $this->callBuildFilter(['status' => 'active']);
        $this->assertSame('data @> ?::jsonb', $clause);
        $this->assertSame([json_encode(['status' => 'active'])], $params);
    }

    public function testFilterEmptyUnchanged(): void
    {
        [$clause, $params] = $this->callBuildFilter(null);
        $this->assertSame('', $clause);
        $this->assertSame([], $params);
    }

    public function testFilterInvalidKey(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid filter key');
        $this->callBuildFilter(['bad;key' => ['$gt' => 1]]);
    }

    public function testFilterUnknownOperator(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown filter operator');
        $this->callBuildFilter(['x' => ['$unknown' => 1]]);
    }

    // ========================================================================
    // $project
    // ========================================================================

    public function testProjectIncludeFields(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("_id", $sql);
                $this->assertStringContainsString("data->>'name' AS name", $sql);
                $this->assertStringContainsString("data->>'status' AS status", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$project' => ['name' => 1, 'status' => 1]],
        ]);
    }

    public function testProjectExcludeId(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("data->>'name' AS name", $sql);
                $this->assertStringContainsString("data->>'price' AS price", $sql);
                // _id should not appear
                $this->assertStringNotContainsString('_id', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$project' => ['_id' => 0, 'name' => 1, 'price' => 1]],
        ]);
    }

    public function testProjectRenameViaFieldRef(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("data->>'name' AS customer_name", $sql);
                $this->assertStringContainsString("data->>'amount' AS total", $sql);
                $this->assertStringNotContainsString('_id', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$project' => ['_id' => 0, 'customer_name' => '$name', 'total' => '$amount']],
        ]);
    }

    public function testProjectDotNotation(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("data->'address'->>'city' AS city", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$project' => ['_id' => 0, 'city' => '$address.city']],
        ]);
    }

    public function testProjectRejectsInvalidField(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docAggregate($pdo, 'orders', [
            ['$project' => ['bad field!' => 1]],
        ]);
    }

    // ========================================================================
    // $unwind
    // ========================================================================

    public function testUnwindStringSyntax(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("CROSS JOIN jsonb_array_elements_text(data->'tags') AS _uw_tags", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$unwind' => '$tags'],
        ]);
    }

    public function testUnwindArraySyntax(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("CROSS JOIN jsonb_array_elements_text(data->'items') AS _uw_items", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$unwind' => ['path' => '$items']],
        ]);
    }

    public function testUnwindWithGroup(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("_uw_tags AS _id", $sql);
                $this->assertStringContainsString("GROUP BY _uw_tags", $sql);
                $this->assertStringContainsString("CROSS JOIN jsonb_array_elements_text(data->'tags') AS _uw_tags", $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$unwind' => '$tags'],
            ['$group' => [
                '_id' => '$tags',
                'cnt' => ['$count' => true],
            ]],
        ]);
    }

    public function testUnwindRejectsInvalidField(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docAggregate($pdo, 'orders', [
            ['$unwind' => '$bad field!'],
        ]);
    }

    // ========================================================================
    // $lookup
    // ========================================================================

    public function testLookupCorrelatedSubquery(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString(
                    "COALESCE((SELECT json_agg(users.data) FROM users WHERE users.data->>'uid' = orders.data->>'user_id'), '[]'::json) AS user_docs",
                    $sql
                );
                return true;
            }))
            ->willReturn($stmt);

        Utils::docAggregate($pdo, 'orders', [
            ['$lookup' => [
                'from' => 'users',
                'localField' => 'user_id',
                'foreignField' => 'uid',
                'as' => 'user_docs',
            ]],
        ]);
    }

    public function testLookupRejectsInvalidIdentifiers(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docAggregate($pdo, 'orders', [
            ['$lookup' => [
                'from' => 'bad table!',
                'localField' => 'user_id',
                'foreignField' => 'uid',
                'as' => 'user_docs',
            ]],
        ]);
    }

    public function testLookupWithMatch(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                $this->assertStringContainsString(
                    "COALESCE((SELECT json_agg(products.data) FROM products WHERE products.data->>'pid' = orders.data->>'product_id'), '[]'::json) AS product_info",
                    $sql
                );
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['status' => 'active'])]);

        Utils::docAggregate($pdo, 'orders', [
            ['$match' => ['status' => 'active']],
            ['$lookup' => [
                'from' => 'products',
                'localField' => 'product_id',
                'foreignField' => 'pid',
                'as' => 'product_info',
            ]],
        ]);
    }

    public function testUnwindGroupSortPipeline(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("_uw_items AS _id", $sql);
                $this->assertStringContainsString("CROSS JOIN jsonb_array_elements_text(data->'items') AS _uw_items", $sql);
                $this->assertStringContainsString("GROUP BY _uw_items", $sql);
                $this->assertStringContainsString("WHERE data @> ?::jsonb", $sql);
                $this->assertStringContainsString("ORDER BY total DESC", $sql);
                $this->assertStringContainsString("LIMIT ?", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with([json_encode(['status' => 'complete']), 5]);

        Utils::docAggregate($pdo, 'orders', [
            ['$match' => ['status' => 'complete']],
            ['$unwind' => '$items'],
            ['$group' => [
                '_id' => '$items',
                'total' => ['$sum' => '$amount'],
                'cnt' => ['$count' => true],
            ]],
            ['$sort' => ['total' => -1]],
            ['$limit' => 5],
        ]);
    }

    // ========================================================================
    // Dot notation expansion in plain containment filters
    // ========================================================================

    public function testExpandDotKeysSingleLevel(): void
    {
        $result = Utils::expandDotKeys(['addr.city' => 'NY']);
        $this->assertSame(['addr' => ['city' => 'NY']], $result);
    }

    public function testExpandDotKeysDeepNesting(): void
    {
        $result = Utils::expandDotKeys(['a.b.c' => 1]);
        $this->assertSame(['a' => ['b' => ['c' => 1]]], $result);
    }

    public function testExpandDotKeysMixedWithPlain(): void
    {
        $result = Utils::expandDotKeys(['status' => 'active', 'addr.city' => 'NY']);
        $this->assertSame(['status' => 'active', 'addr' => ['city' => 'NY']], $result);
    }

    public function testExpandDotKeysMergeSiblings(): void
    {
        $result = Utils::expandDotKeys(['a.b' => 1, 'a.c' => 2]);
        $this->assertSame(['a' => ['b' => 1, 'c' => 2]], $result);
    }

    public function testExpandDotKeysNoDotsUnchanged(): void
    {
        $result = Utils::expandDotKeys(['status' => 'active']);
        $this->assertSame(['status' => 'active'], $result);
    }

    public function testDotNotationWithOperators(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                $this->assertStringContainsString('::numeric >', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with($this->callback(function (array $params) {
                $this->assertSame(['addr' => ['city' => 'NY']], json_decode($params[0], true));
                $this->assertSame(25, $params[1]);
                return true;
            }));

        Utils::docFind($pdo, 'users', ['addr.city' => 'NY', 'age' => ['$gt' => 25]]);
    }

    public function testDotNotationInDocFind(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with($this->callback(function (array $params) {
                $this->assertSame(['addr' => ['city' => 'NY']], json_decode($params[0], true));
                return true;
            }));

        Utils::docFind($pdo, 'users', ['addr.city' => 'NY']);
    }

    public function testDotNotationInDocCount(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(3);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('WHERE data @> ?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')
            ->with($this->callback(function (array $params) {
                $this->assertSame(['addr' => ['city' => 'NY']], json_decode($params[0], true));
                return true;
            }));

        $result = Utils::docCount($pdo, 'users', ['addr.city' => 'NY']);
        $this->assertSame(3, $result);
    }

    // ========================================================================
    // docWatch
    // ========================================================================

    public function testDocWatchCreatesTriggerFunctionAndListen(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docWatch($pdo, 'orders');

        $this->assertCount(4, $execCalls);
        $this->assertStringContainsString('CREATE OR REPLACE FUNCTION orders_notify_fn', $execCalls[0]);
        $this->assertStringContainsString('pg_notify', $execCalls[0]);
        $this->assertStringContainsString('TG_OP', $execCalls[0]);
        $this->assertStringContainsString('DROP TRIGGER IF EXISTS orders_notify_trg ON orders', $execCalls[1]);
        $this->assertStringContainsString('CREATE TRIGGER orders_notify_trg', $execCalls[2]);
        $this->assertStringContainsString('AFTER INSERT OR UPDATE OR DELETE ON orders', $execCalls[2]);
        $this->assertStringContainsString('EXECUTE FUNCTION orders_notify_fn()', $execCalls[2]);
        $this->assertStringContainsString('LISTEN orders_changes', $execCalls[3]);
    }

    public function testDocWatchInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::docWatch($pdo, 'DROP;--');
    }

    // ========================================================================
    // docUnwatch
    // ========================================================================

    public function testDocUnwatchDropsTriggerFunctionAndUnlisten(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docUnwatch($pdo, 'orders');

        $this->assertCount(3, $execCalls);
        $this->assertStringContainsString('DROP TRIGGER IF EXISTS orders_notify_trg ON orders', $execCalls[0]);
        $this->assertStringContainsString('DROP FUNCTION IF EXISTS orders_notify_fn()', $execCalls[1]);
        $this->assertStringContainsString('UNLISTEN orders_changes', $execCalls[2]);
    }

    public function testDocUnwatchInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docUnwatch($pdo, '1bad');
    }

    // ========================================================================
    // docCreateTtlIndex
    // ========================================================================

    public function testDocCreateTtlIndexDefaultField(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docCreateTtlIndex($pdo, 'sessions', 3600);

        $this->assertCount(4, $execCalls);
        $this->assertStringContainsString('CREATE INDEX IF NOT EXISTS sessions_ttl_idx ON sessions (created_at)', $execCalls[0]);
        $this->assertStringContainsString('CREATE OR REPLACE FUNCTION sessions_ttl_fn', $execCalls[1]);
        $this->assertStringContainsString("INTERVAL '3600 seconds'", $execCalls[1]);
        $this->assertStringContainsString('DELETE FROM sessions WHERE created_at', $execCalls[1]);
        $this->assertStringContainsString('DROP TRIGGER IF EXISTS sessions_ttl_trg ON sessions', $execCalls[2]);
        $this->assertStringContainsString('CREATE TRIGGER sessions_ttl_trg', $execCalls[3]);
        $this->assertStringContainsString('BEFORE INSERT ON sessions', $execCalls[3]);
    }

    public function testDocCreateTtlIndexCustomField(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docCreateTtlIndex($pdo, 'tokens', 86400, 'expires_at');

        $this->assertStringContainsString('ON tokens (expires_at)', $execCalls[0]);
        $this->assertStringContainsString('WHERE expires_at', $execCalls[1]);
    }

    public function testDocCreateTtlIndexRejectsNonPositive(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('expireAfterSeconds must be a positive integer');
        Utils::docCreateTtlIndex($pdo, 'items', 0);
    }

    // ========================================================================
    // docRemoveTtlIndex
    // ========================================================================

    public function testDocRemoveTtlIndexDropsAll(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docRemoveTtlIndex($pdo, 'sessions');

        $this->assertCount(3, $execCalls);
        $this->assertStringContainsString('DROP TRIGGER IF EXISTS sessions_ttl_trg ON sessions', $execCalls[0]);
        $this->assertStringContainsString('DROP FUNCTION IF EXISTS sessions_ttl_fn()', $execCalls[1]);
        $this->assertStringContainsString('DROP INDEX IF EXISTS sessions_ttl_idx', $execCalls[2]);
    }

    // ========================================================================
    // docCreateCapped
    // ========================================================================

    public function testDocCreateCappedEnsuresCollectionAndCreatesTrigger(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docCreateCapped($pdo, 'logs', 1000);

        // ensureCollection (1) + CREATE FUNCTION (2) + DROP TRIGGER (3) + CREATE TRIGGER (4)
        $this->assertCount(4, $execCalls);
        $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS logs', $execCalls[0]);
        $this->assertStringContainsString('CREATE OR REPLACE FUNCTION logs_cap_fn', $execCalls[1]);
        $this->assertStringContainsString('DELETE FROM logs', $execCalls[1]);
        $this->assertStringContainsString('ORDER BY created_at ASC', $execCalls[1]);
        $this->assertStringContainsString('LIMIT GREATEST', $execCalls[1]);
        $this->assertStringContainsString('1000', $execCalls[1]);
        $this->assertStringContainsString('DROP TRIGGER IF EXISTS logs_cap_trg ON logs', $execCalls[2]);
        $this->assertStringContainsString('CREATE TRIGGER logs_cap_trg', $execCalls[3]);
        $this->assertStringContainsString('AFTER INSERT ON logs', $execCalls[3]);
    }

    public function testDocCreateCappedRejectsNonPositive(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('maxDocuments must be a positive integer');
        Utils::docCreateCapped($pdo, 'logs', 0);
    }

    // ========================================================================
    // docRemoveCap
    // ========================================================================

    public function testDocRemoveCapDropsTriggerAndFunction(): void
    {
        $execCalls = [];
        $pdo = $this->makeMockPDO();
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        Utils::docRemoveCap($pdo, 'logs');

        $this->assertCount(2, $execCalls);
        $this->assertStringContainsString('DROP TRIGGER IF EXISTS logs_cap_trg ON logs', $execCalls[0]);
        $this->assertStringContainsString('DROP FUNCTION IF EXISTS logs_cap_fn()', $execCalls[1]);
    }

    public function testDocRemoveCapInvalidCollection(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::docRemoveCap($pdo, 'bad table');
    }
}
