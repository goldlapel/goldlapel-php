<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class InstanceMethodsTest extends TestCase
{
    private function makeGlWithMockPDO(): array
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $pdo = $this->createMock(\PDO::class);

        // Inject the mock PDO via reflection — the real factory goes
        // through proc_open/PDO construction which we can't do in unit tests.
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        $ref->setValue($gl, $pdo);

        return [$gl, $pdo];
    }

    private function makeMockStmt(array $rows = [], int $rowCount = 0): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn($rows);
        $stmt->method('fetch')->willReturn($rows[0] ?? false);
        $stmt->method('fetchColumn')->willReturn($rows[0]['_count'] ?? false);
        $stmt->method('rowCount')->willReturn($rowCount);
        return $stmt;
    }

    // ========================================================================
    // pdo() getter
    // ========================================================================

    public function testPdoThrowsWhenNotConnected(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->pdo();
    }

    public function testPdoReturnsConnectionAfterInjection(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $this->assertSame($pdo, $gl->pdo());
    }

    // ========================================================================
    // stop() clears PDO
    // ========================================================================

    public function testStopClearsPdo(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $this->assertSame($pdo, $gl->pdo());

        $gl->stop();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->pdo();
    }

    // ========================================================================
    // Document Store instance methods
    // ========================================================================

    public function testDocInsertDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $row = ['_id' => 'abc', 'data' => '{"name":"Alice"}', 'created_at' => '2026-01-01'];
        $stmt = $this->makeMockStmt([$row]);

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->docInsert('users', ['name' => 'Alice']);
        $this->assertSame('abc', $result['_id']);
    }

    public function testDocInsertManyDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [
            ['_id' => 'a', 'data' => '{"n":"A"}', 'created_at' => '2026-01-01'],
            ['_id' => 'b', 'data' => '{"n":"B"}', 'created_at' => '2026-01-01'],
        ];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->docInsertMany('users', [['n' => 'A'], ['n' => 'B']]);
        $this->assertCount(2, $result);
    }

    public function testDocFindDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['_id' => 'abc', 'data' => '{"name":"Alice"}', 'created_at' => '2026-01-01']];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->docFind('users', ['name' => 'Alice']);
        $this->assertCount(1, $result);
    }

    public function testDocFindOneDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $row = ['_id' => 'abc', 'data' => '{"name":"Alice"}', 'created_at' => '2026-01-01'];
        $stmt = $this->makeMockStmt([$row]);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->docFindOne('users', ['name' => 'Alice']);
        $this->assertSame('abc', $result['_id']);
    }

    public function testDocUpdateDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt([], 2);

        $pdo->method('prepare')->willReturn($stmt);

        $count = $gl->docUpdate('users', ['active' => true], ['active' => false]);
        $this->assertSame(2, $count);
    }

    public function testDocUpdateOneDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt([], 1);

        $pdo->method('prepare')->willReturn($stmt);

        $count = $gl->docUpdateOne('users', ['name' => 'Alice'], ['name' => 'Bob']);
        $this->assertSame(1, $count);
    }

    public function testDocDeleteDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt([], 3);

        $pdo->method('prepare')->willReturn($stmt);

        $count = $gl->docDelete('users', ['active' => false]);
        $this->assertSame(3, $count);
    }

    public function testDocDeleteOneDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt([], 1);

        $pdo->method('prepare')->willReturn($stmt);

        $count = $gl->docDeleteOne('users', ['name' => 'Alice']);
        $this->assertSame(1, $count);
    }

    public function testDocCountDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(42);

        $pdo->method('prepare')->willReturn($stmt);

        $count = $gl->docCount('users');
        $this->assertSame(42, $count);
    }

    public function testDocCreateIndexDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();

        $pdo->expects($this->once())->method('exec')
            ->with($this->callback(function (string $sql) {
                return str_contains($sql, 'CREATE INDEX IF NOT EXISTS users_data_gin');
            }));

        $gl->docCreateIndex('users');
    }

    public function testDocAggregateDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['_id' => 'NYC', 'total' => 5]];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->docAggregate('users', [
            ['$group' => ['_id' => '$city', 'total' => ['$count' => true]]],
        ]);
        $this->assertCount(1, $result);
    }

    // ========================================================================
    // Search instance methods
    // ========================================================================

    public function testSearchDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['id' => 1, 'title' => 'Hello', '_score' => 0.5]];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->search('articles', 'title', 'hello');
        $this->assertCount(1, $result);
    }

    public function testSearchFuzzyDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['id' => 1, 'name' => 'alice', '_score' => 0.8]];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->searchFuzzy('users', 'name', 'alce');
        $this->assertCount(1, $result);
    }

    public function testSuggestDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['id' => 1, 'name' => 'alice', '_score' => 0.7]];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->suggest('users', 'name', 'ali');
        $this->assertCount(1, $result);
    }

    // ========================================================================
    // Pub/Sub & Queue instance methods
    // ========================================================================

    public function testPublishDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt();

        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(fn($sql) => str_contains($sql, 'pg_notify')))
            ->willReturn($stmt);

        $gl->publish('events', 'hello');
    }

    public function testEnqueueDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt();

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $gl->enqueue('jobs', ['task' => 'send_email']);
        $this->assertTrue(true); // No exception = pass
    }

    public function testDequeueDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetch')->willReturn([json_encode(['task' => 'send_email'])]);

        $pdo->method('query')->willReturn($stmt);

        $result = $gl->dequeue('jobs');
        $this->assertSame(['task' => 'send_email'], $result);
    }

    // ========================================================================
    // Counters instance methods
    // ========================================================================

    public function testIncrDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(5);

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->incr('counters', 'page_views');
        $this->assertSame(5, $result);
    }

    public function testGetCounterDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(42);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->getCounter('counters', 'page_views');
        $this->assertSame(42, $result);
    }

    // ========================================================================
    // Hash instance methods
    // ========================================================================

    public function testHsetDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt();

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $gl->hset('cache', 'user:1', 'name', 'Alice');
        $this->assertTrue(true);
    }

    public function testHgetDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn('Alice');

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->hget('cache', 'user:1', 'name');
        $this->assertSame('Alice', $result);
    }

    public function testHgetallDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn('{"name":"Alice","age":"30"}');

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->hgetall('cache', 'user:1');
        $this->assertSame(['name' => 'Alice', 'age' => '30'], $result);
    }

    // ========================================================================
    // Sorted Set instance methods
    // ========================================================================

    public function testZaddDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->makeMockStmt();

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $gl->zadd('leaderboard', 'alice', 100.0);
        $this->assertTrue(true);
    }

    public function testZrangeDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['alice', 100.0], ['bob', 90.0]];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->zrange('leaderboard', 0, 10);
        $this->assertCount(2, $result);
    }

    public function testZscoreDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(100.0);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->zscore('leaderboard', 'alice');
        $this->assertSame(100.0, $result);
    }

    // ========================================================================
    // Stream instance methods
    // ========================================================================

    public function testStreamAddDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1);

        $pdo->method('exec')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $id = $gl->streamAdd('events', ['type' => 'click']);
        $this->assertSame(1, $id);
    }

    // ========================================================================
    // Percolate instance methods
    // ========================================================================

    public function testPercolateDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['query_id' => 'q1', 'query_text' => 'test', 'metadata' => null, '_score' => 0.5]];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->percolate('alerts', 'some text to test');
        $this->assertCount(1, $result);
        $this->assertSame('q1', $result[0]['query_id']);
    }

    // ========================================================================
    // Debug instance methods
    // ========================================================================

    public function testAnalyzeDelegates(): void
    {
        [$gl, $pdo] = $this->makeGlWithMockPDO();
        $rows = [['alias' => 'asciiword', 'description' => 'Word', 'token' => 'hello', 'dictionaries' => '{english_stem}', 'dictionary' => 'english_stem', 'lexemes' => '{hello}']];
        $stmt = $this->makeMockStmt($rows);

        $pdo->method('prepare')->willReturn($stmt);

        $result = $gl->analyze('hello world');
        $this->assertCount(1, $result);
    }

    // ========================================================================
    // Instance methods throw when not connected
    // ========================================================================

    public function testInstanceMethodThrowsWhenNotConnected(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->docInsert('users', ['name' => 'Alice']);
    }

    public function testSearchThrowsWhenNotConnected(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->search('articles', 'title', 'hello');
    }

    public function testIncrThrowsWhenNotConnected(): void
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not connected');
        $gl->incr('counters', 'views');
    }
}
