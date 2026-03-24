<?php

namespace GoldLapel\Tests;

use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

class NativeCacheTest extends TestCase
{
    protected function setUp(): void
    {
        NativeCache::reset();
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
    }

    private function makeCache(?int $maxEntries = null): NativeCache
    {
        $cache = new NativeCache($maxEntries);
        $cache->setConnected(true);
        return $cache;
    }

    // --- detectWrite ---

    public function testDetectWriteInsert(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('INSERT INTO orders VALUES (1)'));
    }

    public function testDetectWriteInsertSchema(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('INSERT INTO public.orders VALUES (1)'));
    }

    public function testDetectWriteUpdate(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('UPDATE orders SET name = \'x\''));
    }

    public function testDetectWriteDelete(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('DELETE FROM orders WHERE id = 1'));
    }

    public function testDetectWriteTruncate(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('TRUNCATE orders'));
    }

    public function testDetectWriteTruncateTable(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('TRUNCATE TABLE orders'));
    }

    public function testDetectWriteCreateDdl(): void
    {
        $this->assertSame(NativeCache::DDL_SENTINEL, NativeCache::detectWrite('CREATE TABLE foo (id int)'));
    }

    public function testDetectWriteAlterDdl(): void
    {
        $this->assertSame(NativeCache::DDL_SENTINEL, NativeCache::detectWrite('ALTER TABLE foo ADD COLUMN bar int'));
    }

    public function testDetectWriteDropDdl(): void
    {
        $this->assertSame(NativeCache::DDL_SENTINEL, NativeCache::detectWrite('DROP TABLE foo'));
    }

    public function testDetectWriteSelectReturnsNull(): void
    {
        $this->assertNull(NativeCache::detectWrite('SELECT * FROM orders'));
    }

    public function testDetectWriteCaseInsensitive(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite('insert INTO Orders VALUES (1)'));
    }

    public function testDetectWriteCopyFrom(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite("COPY orders FROM '/tmp/data.csv'"));
    }

    public function testDetectWriteCopyToNull(): void
    {
        $this->assertNull(NativeCache::detectWrite("COPY orders TO '/tmp/data.csv'"));
    }

    public function testDetectWriteCopySubqueryNull(): void
    {
        $this->assertNull(NativeCache::detectWrite("COPY (SELECT * FROM orders) TO '/tmp/data.csv'"));
    }

    public function testDetectWriteWithCteInsert(): void
    {
        $this->assertSame(NativeCache::DDL_SENTINEL, NativeCache::detectWrite('WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x'));
    }

    public function testDetectWriteWithCteSelect(): void
    {
        $this->assertNull(NativeCache::detectWrite('WITH x AS (SELECT 1) SELECT * FROM x'));
    }

    public function testDetectWriteEmpty(): void
    {
        $this->assertNull(NativeCache::detectWrite(''));
    }

    public function testDetectWriteWhitespace(): void
    {
        $this->assertNull(NativeCache::detectWrite('   '));
    }

    public function testDetectWriteCopyWithColumns(): void
    {
        $this->assertSame('orders', NativeCache::detectWrite("COPY orders(id, name) FROM '/tmp/data.csv'"));
    }

    // --- extractTables ---

    public function testExtractTablesSimpleFrom(): void
    {
        $this->assertContains('orders', NativeCache::extractTables('SELECT * FROM orders'));
    }

    public function testExtractTablesJoin(): void
    {
        $tables = NativeCache::extractTables('SELECT * FROM orders o JOIN customers c ON o.cid = c.id');
        $this->assertContains('orders', $tables);
        $this->assertContains('customers', $tables);
    }

    public function testExtractTablesSchemaQualified(): void
    {
        $this->assertContains('orders', NativeCache::extractTables('SELECT * FROM public.orders'));
    }

    public function testExtractTablesMultipleJoins(): void
    {
        $tables = NativeCache::extractTables('SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1');
        $this->assertCount(3, $tables);
    }

    public function testExtractTablesCaseInsensitive(): void
    {
        $this->assertContains('orders', NativeCache::extractTables('SELECT * FROM ORDERS'));
    }

    public function testExtractTablesNoTables(): void
    {
        $this->assertCount(0, NativeCache::extractTables('SELECT 1'));
    }

    public function testExtractTablesSubquery(): void
    {
        $tables = NativeCache::extractTables('SELECT * FROM orders WHERE id IN (SELECT oid FROM users)');
        $this->assertContains('orders', $tables);
        $this->assertContains('users', $tables);
    }

    // --- Transaction detection ---

    public function testTxStartBegin(): void
    {
        $this->assertTrue(NativeCache::isTxStart('BEGIN'));
    }

    public function testTxStartTransaction(): void
    {
        $this->assertTrue(NativeCache::isTxStart('START TRANSACTION'));
    }

    public function testTxEndCommit(): void
    {
        $this->assertTrue(NativeCache::isTxEnd('COMMIT'));
    }

    public function testTxEndRollback(): void
    {
        $this->assertTrue(NativeCache::isTxEnd('ROLLBACK'));
    }

    public function testTxEndEnd(): void
    {
        $this->assertTrue(NativeCache::isTxEnd('END'));
    }

    public function testSavepointNotTxStart(): void
    {
        $this->assertFalse(NativeCache::isTxStart('SAVEPOINT x'));
    }

    public function testSelectNotTxStart(): void
    {
        $this->assertFalse(NativeCache::isTxStart('SELECT 1'));
    }

    // --- Cache operations ---

    public function testPutAndGet(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM users', null, [['id' => '1', 'name' => 'alice']], ['id', 'name']);
        $entry = $cache->get('SELECT * FROM users', null);
        $this->assertNotNull($entry);
        $this->assertCount(1, $entry['rows']);
    }

    public function testMissReturnsNull(): void
    {
        $cache = $this->makeCache();
        $this->assertNull($cache->get('SELECT 1', null));
    }

    public function testParamsDifferentiate(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT $1', [1], [['id' => '1']], ['id']);
        $cache->put('SELECT $1', [2], [['id' => '2']], ['id']);
        $this->assertSame('1', $cache->get('SELECT $1', [1])['rows'][0]['id']);
        $this->assertSame('2', $cache->get('SELECT $1', [2])['rows'][0]['id']);
    }

    public function testStats(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT 1', null, [['x' => '1']], ['x']);
        $cache->get('SELECT 1', null);
        $cache->get('SELECT 2', null);
        $this->assertSame(1, $cache->statsHits);
        $this->assertSame(1, $cache->statsMisses);
    }

    public function testDisabledCacheReturnsNull(): void
    {
        $cache = new NativeCache(null, false);
        $cache->setConnected(true);
        $cache->put('SELECT * FROM users', null, [['id' => '1']], ['id']);
        $this->assertNull($cache->get('SELECT * FROM users', null));
    }

    public function testNotConnectedReturnsNull(): void
    {
        $cache = new NativeCache();
        // Not connected
        $cache->put('SELECT * FROM users', null, [['id' => '1']], ['id']);
        $this->assertNull($cache->get('SELECT * FROM users', null));
    }

    // --- Invalidation ---

    public function testInvalidateTable(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);
        $cache->put('SELECT * FROM users', null, [['id' => '2']], ['id']);
        $cache->invalidateTable('orders');
        $this->assertNull($cache->get('SELECT * FROM orders', null));
        $this->assertNotNull($cache->get('SELECT * FROM users', null));
    }

    public function testInvalidateAll(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);
        $cache->put('SELECT * FROM users', null, [['id' => '2']], ['id']);
        $cache->invalidateAll();
        $this->assertNull($cache->get('SELECT * FROM orders', null));
        $this->assertNull($cache->get('SELECT * FROM users', null));
    }

    public function testCrossReferenced(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders JOIN users ON 1=1', null, [['id' => '1']], ['id']);
        $cache->invalidateTable('orders');
        $this->assertNull($cache->get('SELECT * FROM orders JOIN users ON 1=1', null));
    }

    public function testCrossReferencedCleansOtherTableIndex(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders JOIN users ON 1=1', null, [['id' => '1']], ['id']);
        $cache->invalidateTable('orders');
        // After invalidating orders, the users table index should also be cleaned
        $this->assertSame(0, $cache->size());
    }

    // --- Signal processing ---

    public function testTableSignal(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);
        $cache->processSignal('I:orders');
        $this->assertNull($cache->get('SELECT * FROM orders', null));
    }

    public function testWildcardSignal(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);
        $cache->processSignal('I:*');
        $this->assertNull($cache->get('SELECT * FROM orders', null));
    }

    public function testKeepalivePreserves(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);
        $cache->processSignal('P:');
        $this->assertNotNull($cache->get('SELECT * FROM orders', null));
    }

    public function testUnknownSignalPreserves(): void
    {
        $cache = $this->makeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);
        $cache->processSignal('X:something');
        $this->assertNotNull($cache->get('SELECT * FROM orders', null));
    }

    // --- LRU eviction ---

    public function testEvictionRemovesLru(): void
    {
        $cache = $this->makeCache(2);
        $cache->put('SELECT * FROM a', null, [['id' => '1']], ['id']);
        $cache->put('SELECT * FROM b', null, [['id' => '2']], ['id']);

        // Access 'a' to make it more recent
        $cache->get('SELECT * FROM a', null);

        // Adding a third should evict 'b' (the least recently used)
        $cache->put('SELECT * FROM c', null, [['id' => '3']], ['id']);

        $this->assertNotNull($cache->get('SELECT * FROM a', null));
        $this->assertNull($cache->get('SELECT * FROM b', null));
        $this->assertNotNull($cache->get('SELECT * FROM c', null));
    }

    // --- Singleton ---

    public function testGetInstance(): void
    {
        $a = NativeCache::getInstance();
        $b = NativeCache::getInstance();
        $this->assertSame($a, $b);
    }

    public function testResetClearsSingleton(): void
    {
        $a = NativeCache::getInstance();
        NativeCache::reset();
        $b = NativeCache::getInstance();
        $this->assertNotSame($a, $b);
    }

    // --- Push invalidation (TCP) ---

    public function testConnectInvalidationSetsConnected(): void
    {
        $cache = new NativeCache();
        $this->assertFalse($cache->isConnected());

        // Create a server to accept the connection
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);
        $name = stream_socket_get_name($server, false);
        $port = (int) explode(':', $name)[1];

        // Accept in background (non-blocking)
        stream_set_blocking($server, false);

        // Connect invalidation
        $cache->connectInvalidation($port);

        fclose($server);

        // After successful connection, cache should be connected
        $this->assertTrue($cache->isConnected());
    }

    public function testConnectInvalidationFailsGracefully(): void
    {
        $cache = new NativeCache();
        // Connect to a port where nothing is listening
        $cache->connectInvalidation(19999);
        // Should not throw, just remain disconnected
        $this->assertFalse($cache->isConnected());
    }

    public function testConnectInvalidationDrainsSignals(): void
    {
        $cache = new NativeCache();
        $cache->put('SELECT * FROM orders', null, [['id' => '1']], ['id']);

        // Create a server, pre-write the signal, then let the cache connect
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);
        $name = stream_socket_get_name($server, false);
        $port = (int) explode(':', $name)[1];

        // Fork: child accepts and writes signal immediately
        $pid = pcntl_fork();
        if ($pid === 0) {
            $conn = stream_socket_accept($server, 2);
            if ($conn) {
                fwrite($conn, "I:orders\n");
                fflush($conn);
                // Keep connection open briefly so parent can read
                usleep(500000);
                fclose($conn);
            }
            fclose($server);
            exit(0);
        }

        fclose($server);

        // Small delay to let child accept and write
        usleep(200000);

        // Now connect — drainSignals should pick up the pending "I:orders"
        $cache->connectInvalidation($port);

        pcntl_waitpid($pid, $status);

        // The signal should have been processed, invalidating the cache entry
        $this->assertNull($cache->get('SELECT * FROM orders', null));
    }

    // --- makeKey ---

    public function testMakeKeyNullParams(): void
    {
        $key = NativeCache::makeKey('SELECT 1', null);
        $this->assertSame("SELECT 1\0null", $key);
    }

    public function testMakeKeyEmptyParams(): void
    {
        $key = NativeCache::makeKey('SELECT 1', []);
        $this->assertSame("SELECT 1\0null", $key);
    }

    public function testMakeKeyWithParams(): void
    {
        $key1 = NativeCache::makeKey('SELECT $1', [1]);
        $key2 = NativeCache::makeKey('SELECT $1', [2]);
        $this->assertNotSame($key1, $key2);
    }

    // --- bareTable ---

    public function testBareTableSimple(): void
    {
        $this->assertSame('orders', NativeCache::bareTable('orders'));
    }

    public function testBareTableSchemaQualified(): void
    {
        $this->assertSame('orders', NativeCache::bareTable('public.orders'));
    }

    public function testBareTableWithParens(): void
    {
        $this->assertSame('orders', NativeCache::bareTable('orders(id, name)'));
    }

    public function testBareTableUpperCase(): void
    {
        $this->assertSame('orders', NativeCache::bareTable('ORDERS'));
    }
}
