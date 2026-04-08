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
        $this->expectExceptionMessage('Unsupported pipeline stage: $unwind');
        Utils::docAggregate($pdo, 'users', [
            ['$unwind' => '$tags'],
        ]);
    }

    public function testDocAggregateUnsupportedAccumulator(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported accumulator: $push');
        Utils::docAggregate($pdo, 'users', [
            ['$group' => [
                '_id' => '$status',
                'names' => ['$push' => '$name'],
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
}
