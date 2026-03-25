<?php

namespace GoldLapel\Tests;

use GoldLapel\CachedPDO;
use GoldLapel\CachedPDOStatement;
use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

class CachedPDOTest extends TestCase
{
    private NativeCache $cache;

    protected function setUp(): void
    {
        NativeCache::reset();
        $this->cache = new NativeCache();
        $this->cache->setConnected(true);
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
    }

    private function makeMockPDO(): \PDO
    {
        return $this->createMock(\PDO::class);
    }

    private function makeMockStmt(array $rows, int $colCount = 0): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn($rows);
        $stmt->method('columnCount')->willReturn($colCount > 0 ? $colCount : (empty($rows) ? 0 : count(array_keys($rows[0]))));
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(count($rows));
        return $stmt;
    }

    // --- query() caching ---

    public function testQueryCachesSelectResult(): void
    {
        $rows = [
            ['id' => 1, 'name' => 'alice'],
            ['id' => 2, 'name' => 'bob'],
        ];

        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);

        // query() should only be called once — second call hits cache
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $stmt1 = $cached->query('SELECT * FROM users');
        $rows1 = $stmt1->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows1);

        // Second call should return from cache
        $stmt2 = $cached->query('SELECT * FROM users');
        $rows2 = $stmt2->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows2);
        $this->assertSame(1, $this->cache->statsHits);
    }

    public function testQueryWriteInvalidatesCache(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];
        $rowsAfter = [['id' => 1, 'name' => 'alice'], ['id' => 3, 'name' => 'charlie']];

        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 2);
        $stmt2 = $this->makeMockStmt($rowsAfter, 2);
        $insertStmt = $this->makeMockStmt([], 0);

        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $insertStmt, $stmt2);
        $pdo->method('exec')->willReturn(1);

        $cached = new CachedPDO($pdo, $this->cache);

        // Prime cache
        $cached->query('SELECT * FROM users');

        // Write invalidates
        $cached->exec("INSERT INTO users VALUES (3, 'charlie')");

        // Should be a miss now
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
    }

    public function testQueryDdlInvalidatesAll(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);
        $stmt2 = $this->makeMockStmt($rows, 1);

        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $stmt2);
        $pdo->method('exec')->willReturn(0);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $cached->exec('CREATE TABLE IF NOT EXISTS other (id INTEGER)');

        // Cache should be cleared
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
    }

    // --- prepare() + execute() caching ---

    public function testPrepareExecuteCachesResult(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];

        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);

        $pdo->method('prepare')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $stmt = $cached->prepare('SELECT * FROM users WHERE id = ?');
        $stmt->execute([1]);
        $rows1 = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows1);

        // Second execution with same params should hit cache
        $stmt2 = $cached->prepare('SELECT * FROM users WHERE id = ?');
        $stmt2->execute([1]);
        $rows2 = $stmt2->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows2);
        $this->assertSame(1, $this->cache->statsHits);
    }

    public function testPrepareExecuteDifferentParams(): void
    {
        $rows1 = [['id' => 1, 'name' => 'alice']];
        $rows2 = [['id' => 2, 'name' => 'bob']];

        $pdo = $this->makeMockPDO();
        $realStmt1 = $this->makeMockStmt($rows1, 2);
        $realStmt2 = $this->makeMockStmt($rows2, 2);

        $pdo->method('prepare')->willReturnOnConsecutiveCalls($realStmt1, $realStmt2);

        $cached = new CachedPDO($pdo, $this->cache);

        $stmt1 = $cached->prepare('SELECT * FROM users WHERE id = ?');
        $stmt1->execute([1]);
        $stmt1->fetchAll();

        $stmt2 = $cached->prepare('SELECT * FROM users WHERE id = ?');
        $stmt2->execute([2]);
        $result = $stmt2->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertSame('bob', $result[0]['name']);
        $this->assertSame(0, $this->cache->statsHits);
    }

    // --- Transaction bypass ---

    public function testTransactionBypassesCache(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);
        $stmt2 = $this->makeMockStmt($rows, 1);

        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $stmt2);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('commit')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);

        // Prime cache
        $cached->query('SELECT * FROM users');

        // Begin transaction - reads should bypass cache
        $cached->beginTransaction();
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
        $cached->commit();
    }

    public function testAfterCommitCacheWorks(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);

        $pdo->expects($this->once())->method('query')->willReturn($stmt1);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('commit')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);

        // Prime cache
        $cached->query('SELECT * FROM users');

        $cached->beginTransaction();
        $cached->commit();

        // After commit, cache should work again
        $cached->query('SELECT * FROM users');
        $this->assertSame(1, $this->cache->statsHits);
    }

    public function testRollbackRestoresCache(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);

        $pdo->expects($this->once())->method('query')->willReturn($stmt1);
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('rollBack')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');

        $cached->beginTransaction();
        $cached->rollBack();

        $cached->query('SELECT * FROM users');
        $this->assertSame(1, $this->cache->statsHits);
    }

    // --- fetch modes ---

    public function testFetchAssoc(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users WHERE id = 1');
        $stmt = $cached->query('SELECT * FROM users WHERE id = 1');
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $this->assertSame('alice', $row['name']);
    }

    public function testFetchNum(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users WHERE id = 1');
        $stmt = $cached->query('SELECT * FROM users WHERE id = 1');
        $row = $stmt->fetch(\PDO::FETCH_NUM);
        $this->assertIsArray($row);
        $this->assertSame(1, $row[0]);
        $this->assertSame('alice', $row[1]);
    }

    public function testFetchColumn(): void
    {
        $rows = [['name' => 'alice']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT name FROM users WHERE id = 1');
        $stmt = $cached->query('SELECT name FROM users WHERE id = 1');
        $val = $stmt->fetchColumn(0);
        $this->assertSame('alice', $val);
    }

    public function testFetchReturnsFalseWhenExhausted(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users WHERE id = 1');
        $stmt = $cached->query('SELECT * FROM users WHERE id = 1');
        $stmt->fetch();
        $this->assertFalse($stmt->fetch());
    }

    // --- rowCount / columnCount ---

    public function testRowCount(): void
    {
        $rows = [['id' => 1], ['id' => 2]];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $stmt = $cached->query('SELECT * FROM users');
        $this->assertSame(2, $stmt->rowCount());
    }

    public function testColumnCount(): void
    {
        $rows = [['id' => 1, 'name' => 'alice', 'email' => 'a@b.c']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 3);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $stmt = $cached->query('SELECT * FROM users');
        $this->assertSame(3, $stmt->columnCount());
    }

    // --- exec() write detection ---

    public function testExecDetectsInsert(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);
        $stmt2 = $this->makeMockStmt($rows, 1);
        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $stmt2);
        $pdo->method('exec')->willReturn(1);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $cached->exec("INSERT INTO users VALUES (3, 'charlie')");
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
    }

    public function testExecDetectsUpdate(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);
        $stmt2 = $this->makeMockStmt($rows, 1);
        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $stmt2);
        $pdo->method('exec')->willReturn(1);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $cached->exec("UPDATE users SET name = 'ALICE' WHERE id = 1");
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
    }

    public function testExecDetectsDelete(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);
        $stmt2 = $this->makeMockStmt($rows, 1);
        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $stmt2);
        $pdo->method('exec')->willReturn(1);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $cached->exec('DELETE FROM users WHERE id = 2');
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
    }

    // --- getWrappedPDO / getCache / unwrap ---

    public function testGetWrappedPDO(): void
    {
        $pdo = $this->makeMockPDO();
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertSame($pdo, $cached->getWrappedPDO());
    }

    public function testUnwrapReturnsSameAsPDO(): void
    {
        $pdo = $this->makeMockPDO();
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertSame($pdo, $cached->unwrap());
        $this->assertSame($cached->getWrappedPDO(), $cached->unwrap());
    }

    public function testGetCache(): void
    {
        $pdo = $this->makeMockPDO();
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertSame($this->cache, $cached->getCache());
    }

    // --- instanceof checks ---

    public function testCachedPDOInstanceOfPDO(): void
    {
        $pdo = $this->makeMockPDO();
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertInstanceOf(\PDO::class, $cached);
    }

    public function testCachedPDOStatementInstanceOfPDOStatement(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);
        $stmt = $cached->query('SELECT * FROM users');
        $this->assertInstanceOf(\PDOStatement::class, $stmt);
    }

    public function testCachedStatementFromCacheInstanceOfPDOStatement(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        // Prime cache
        $cached->query('SELECT * FROM users');

        // Second call returns from cache
        $stmt = $cached->query('SELECT * FROM users');
        $this->assertInstanceOf(\PDOStatement::class, $stmt);
        $this->assertInstanceOf(CachedPDOStatement::class, $stmt);
    }

    public function testCachedPDOAcceptedByPDOTypeHint(): void
    {
        $pdo = $this->makeMockPDO();
        $cached = new CachedPDO($pdo, $this->cache);

        // This function has a PDO type hint — should accept CachedPDO
        $fn = function (\PDO $db): bool { return true; };
        $this->assertTrue($fn($cached));
    }

    // --- PDO passthrough ---

    public function testQuote(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('quote')->willReturn("'it''s'");
        $cached = new CachedPDO($pdo, $this->cache);
        $quoted = $cached->quote("it's");
        $this->assertSame("'it''s'", $quoted);
    }

    public function testLastInsertId(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('lastInsertId')->willReturn('99');
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertSame('99', $cached->lastInsertId());
    }

    public function testErrorCode(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('errorCode')->willReturn('00000');
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertSame('00000', $cached->errorCode());
    }

    public function testErrorInfo(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('errorInfo')->willReturn(['00000', null, null]);
        $cached = new CachedPDO($pdo, $this->cache);
        $this->assertIsArray($cached->errorInfo());
    }

    // --- bindValue ---

    public function testBindValueAndExecute(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);
        $realStmt->method('bindValue')->willReturn(true);
        $pdo->method('prepare')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $stmt = $cached->prepare('SELECT * FROM users WHERE id = ?');
        $stmt->bindValue(1, 1, \PDO::PARAM_INT);
        $stmt->execute();
        $result = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(1, $result);
        $this->assertSame('alice', $result[0]['name']);
    }

    // --- closeCursor ---

    public function testCloseCursorResets(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $stmt = $cached->query('SELECT * FROM users');
        $stmt->closeCursor();
        $this->assertSame(0, $stmt->columnCount());
    }

    // --- Iterator ---

    public function testIterator(): void
    {
        $rows = [['id' => 1], ['id' => 2]];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 1);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users');
        $stmt = $cached->query('SELECT * FROM users');
        $count = 0;
        foreach ($stmt as $row) {
            $count++;
        }
        $this->assertSame(2, $count);
    }

    // --- Self-invalidation on write via prepare/execute ---

    public function testPrepareWriteInvalidates(): void
    {
        $selectRows = [['id' => 1]];
        $pdo = $this->makeMockPDO();

        $selectStmt = $this->makeMockStmt($selectRows, 1);
        $insertStmt = $this->makeMockStmt([], 0);
        $selectStmt2 = $this->makeMockStmt($selectRows, 1);

        $pdo->method('query')->willReturnOnConsecutiveCalls($selectStmt, $selectStmt2);
        $pdo->method('prepare')->willReturn($insertStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        // Prime cache
        $cached->query('SELECT * FROM users');

        // Write via prepare/execute
        $stmt = $cached->prepare('INSERT INTO users VALUES (?, ?)');
        $stmt->execute([3, 'charlie']);

        // Should be a cache miss
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);
    }

    // --- inTransaction tracking ---

    public function testInTransactionTracking(): void
    {
        $pdo = $this->makeMockPDO();
        $pdo->method('beginTransaction')->willReturn(true);
        $pdo->method('commit')->willReturn(true);

        $cached = new CachedPDO($pdo, $this->cache);

        $this->assertFalse($cached->inTransaction());
        $cached->beginTransaction();
        $this->assertTrue($cached->inTransaction());
        $cached->commit();
        $this->assertFalse($cached->inTransaction());
    }

    // --- SQL BEGIN/COMMIT tracking via exec ---

    public function testSqlBeginCommitTrackingViaExec(): void
    {
        $rows = [['id' => 1]];
        $pdo = $this->makeMockPDO();
        $stmt1 = $this->makeMockStmt($rows, 1);
        $stmt2 = $this->makeMockStmt($rows, 1);
        $pdo->method('query')->willReturnOnConsecutiveCalls($stmt1, $stmt2);
        $pdo->method('exec')->willReturn(0);

        $cached = new CachedPDO($pdo, $this->cache);

        // Prime cache
        $cached->query('SELECT * FROM users');

        $cached->exec('BEGIN');
        $this->assertTrue($cached->inTransaction());

        // Read inside transaction bypasses cache
        $cached->query('SELECT * FROM users');
        $this->assertSame(0, $this->cache->statsHits);

        $cached->exec('COMMIT');
        $this->assertFalse($cached->inTransaction());

        // After commit, cache works
        $cached->query('SELECT * FROM users');
        $this->assertSame(1, $this->cache->statsHits);
    }

    // --- fetchAll modes on cached result ---

    public function testFetchAllNum(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users WHERE id = 1');
        $stmt = $cached->query('SELECT * FROM users WHERE id = 1');
        $result = $stmt->fetchAll(\PDO::FETCH_NUM);
        $this->assertSame([1, 'alice'], $result[0]);
    }

    public function testFetchAllBoth(): void
    {
        $rows = [['id' => 1, 'name' => 'alice']];
        $pdo = $this->makeMockPDO();
        $realStmt = $this->makeMockStmt($rows, 2);
        $pdo->expects($this->once())->method('query')->willReturn($realStmt);

        $cached = new CachedPDO($pdo, $this->cache);

        $cached->query('SELECT * FROM users WHERE id = 1');
        $stmt = $cached->query('SELECT * FROM users WHERE id = 1');
        $result = $stmt->fetchAll(\PDO::FETCH_BOTH);
        // Both numeric and assoc keys
        $this->assertSame(1, $result[0][0]);
        $this->assertSame('alice', $result[0]['name']);
    }
}
