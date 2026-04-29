<?php

namespace GoldLapel\Amp\Tests;

use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use ReflectionClass;
use GoldLapel\Amp\GoldLapel;
use GoldLapel\Amp\Utils;
use GoldLapel\Tests\IntegrationGate;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

use function Amp\async;

/**
 * Integration tests for the async Amp wrapper. These talk to a real
 * Postgres and spawn the real goldlapel binary.
 *
 * Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM — the
 * standardized integration-test convention shared across all Gold Lapel
 * wrappers. See tests/IntegrationGate.php. Also requires the goldlapel
 * binary on PATH or GOLDLAPEL_BINARY set.
 *
 * All async operations run inside a fiber via Amp\async() + await().
 * PHPUnit runs each test in the default loop driver — amphp / Revolt
 * installs a default driver on first use, so no Loop::run() wrapper is
 * required.
 */
class IntegrationTest extends TestCase
{
    private static ?string $upstream = null;
    private static bool $proxyAvailable = false;

    public static function setUpBeforeClass(): void
    {
        // Evaluating the gate here surfaces the half-configured CI case
        // (GOLDLAPEL_INTEGRATION=1 set, GOLDLAPEL_TEST_UPSTREAM missing) as
        // a RuntimeException during PHPUnit setup — preventing false-green.
        self::$upstream = IntegrationGate::upstream();
        if (self::$upstream === null) {
            return;
        }

        // Quick TCP liveness check on the upstream host/port
        $parsed = parse_url(self::$upstream);
        if ($parsed === false || !isset($parsed['host'])) {
            return;
        }
        $host = $parsed['host'];
        $port = $parsed['port'] ?? 5432;
        $fp = @fsockopen($host, $port, $errno, $errstr, 1.0);
        if ($fp === false) {
            return;
        }
        fclose($fp);

        // Require the goldlapel binary to be findable
        $envBin = getenv('GOLDLAPEL_BINARY');
        if ($envBin !== false && $envBin !== '' && is_file($envBin)) {
            self::$proxyAvailable = true;
            return;
        }
        // Check PATH
        $paths = explode(PATH_SEPARATOR, getenv('PATH') ?: '');
        foreach ($paths as $p) {
            if (is_file($p . '/goldlapel') && is_executable($p . '/goldlapel')) {
                self::$proxyAvailable = true;
                return;
            }
        }
    }

    protected function setUp(): void
    {
        if (self::$upstream === null) {
            $this->markTestSkipped(IntegrationGate::skipReason());
        }
        if (!self::$proxyAvailable) {
            $this->markTestSkipped(
                'Postgres or goldlapel binary not reachable; set GOLDLAPEL_TEST_UPSTREAM + GOLDLAPEL_BINARY to enable.'
            );
        }
    }

    private function uniqPort(): int
    {
        // Ephemeral range — avoid proxy port collisions between tests.
        // Use a random high port (leaving enough headroom for dashboard
        // port = port + 1 and invalidation = port + 2).
        return random_int(18000, 28000);
    }

    private function uniqColl(): string
    {
        return 'amp_t_' . bin2hex(random_bytes(4));
    }

    private function uniqCh(): string
    {
        return 'amp_ch_' . bin2hex(random_bytes(4));
    }

    /**
     * Open a second connection to the proxy in the same keyword form the
     * main connection uses. Mirrors GoldLapel's private urlToAmphpConnString.
     */
    private function openExtraConnection(GoldLapel $gl): PostgresConnection
    {
        $ref = new ReflectionClass(GoldLapel::class);
        $method = $ref->getMethod('urlToAmphpConnString');
        $method->setAccessible(true);
        $keyword = $method->invoke(null, $gl->url());
        return \Amp\Postgres\connect(PostgresConfig::fromString($keyword));
    }

    private function cleanup(string $table, GoldLapel $gl): void
    {
        try {
            $gl->connection()->query("DROP TABLE IF EXISTS {$table} CASCADE");
        } catch (\Throwable $e) {
            // best-effort
        }
    }

    public function testFactoryStartsProxyAndConnects(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();

        try {
            $this->assertSame($port, $gl->getProxyPort());
            $this->assertNotNull($gl->url());
            $this->assertStringContainsString("localhost:{$port}", $gl->url());
            $this->assertTrue($gl->isRunning());
            $conn = $gl->connection();
            $this->assertInstanceOf(PostgresConnection::class, $conn);

            // Simple round-trip
            $result = $conn->execute('SELECT 1 AS v');
            $row = $result->fetchRow();
            $this->assertSame(1, $row['v']);
        } finally {
            $gl->stop()->await();
        }
        $this->assertFalse($gl->isRunning());
    }

    public function testDocInsertFindAndUpdate(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $coll = $this->uniqColl();
        try {
            $inserted = $gl->documents->insert($coll, ['name' => 'alice', 'age' => 30])->await();
            $this->assertArrayHasKey('_id', $inserted);

            $one = $gl->documents->findOne($coll, ['name' => 'alice'])->await();
            $this->assertNotNull($one);

            $count = $gl->documents->count($coll)->await();
            $this->assertSame(1, $count);

            $affected = $gl->documents->update($coll, ['name' => 'alice'], ['$set' => ['age' => 31]])->await();
            $this->assertSame(1, $affected);

            $one = $gl->documents->findOne($coll, ['name' => 'alice'])->await();
            $data = is_string($one['data']) ? json_decode($one['data'], true) : $one['data'];
            $this->assertSame(31, $data['age']);
        } finally {
            $this->cleanup($coll, $gl);
            $gl->stop()->await();
        }
    }

    public function testJsonbExistsFilterRoundTrip(): void
    {
        // Exercises the `data ? ?` → jsonb_exists rewrite against a real
        // Postgres — verifies the translator doesn't corrupt semantics.
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $coll = $this->uniqColl();
        try {
            $gl->documents->insert($coll, ['email' => 'a@b.com', 'name' => 'alice'])->await();
            $gl->documents->insert($coll, ['name' => 'bob'])->await();

            $withEmail = $gl->documents->find($coll, ['email' => ['$exists' => true]])->await();
            $this->assertCount(1, $withEmail);

            $noEmail = $gl->documents->find($coll, ['email' => ['$exists' => false]])->await();
            $this->assertCount(1, $noEmail);
        } finally {
            $this->cleanup($coll, $gl);
            $gl->stop()->await();
        }
    }

    public function testCounterIncr(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $name = 'amp_counters_' . bin2hex(random_bytes(4));
        try {
            $v1 = $gl->counters->incr($name, 'orders')->await();
            $v2 = $gl->counters->incr($name, 'orders', 5)->await();
            $this->assertSame(1, $v1);
            $this->assertSame(6, $v2);
            $this->assertSame(6, $gl->counters->get($name, 'orders')->await());
        } finally {
            $gl->stop()->await();
        }
    }

    public function testHashOperationsIncludingDelete(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $name = 'amp_hash_' . bin2hex(random_bytes(4));
        try {
            $gl->hashes->set($name, 'user:1', 'name', 'alice')->await();
            $gl->hashes->set($name, 'user:1', 'age', 30)->await();
            $this->assertSame('alice', $gl->hashes->get($name, 'user:1', 'name')->await());
            $this->assertSame(30, $gl->hashes->get($name, 'user:1', 'age')->await());

            $deleted = $gl->hashes->delete($name, 'user:1', 'name')->await();
            $this->assertTrue($deleted);
            $this->assertNull($gl->hashes->get($name, 'user:1', 'name')->await());

            $notDeleted = $gl->hashes->delete($name, 'user:1', 'nonexistent')->await();
            $this->assertFalse($notDeleted);

            $all = $gl->hashes->getAll($name, 'user:1')->await();
            $this->assertArrayHasKey('age', $all);
            $this->assertArrayNotHasKey('name', $all);
        } finally {
            $gl->stop()->await();
        }
    }

    public function testUsingTransactionScope(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $coll = $this->uniqColl();
        $cnt = 'amp_c_' . bin2hex(random_bytes(4));
        try {
            $tx = $gl->connection()->beginTransaction();
            $result = $gl->using($tx, function ($gl) use ($coll, $cnt) {
                $gl->documents->insert($coll, ['type' => 'signup'])->await();
                $gl->counters->incr($cnt, 'signups')->await();
                return 'ok';
            })->await();
            $this->assertSame('ok', $result);
            $tx->commit();

            $this->assertSame(1, $gl->documents->count($coll)->await());
            $this->assertSame(1, $gl->counters->get($cnt, 'signups')->await());
        } finally {
            $this->cleanup($coll, $gl);
            $gl->stop()->await();
        }
    }

    public function testDocFindCursorStreamsInBatches(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $coll = $this->uniqColl();
        try {
            // Insert enough to span multiple batches
            $docs = [];
            for ($i = 0; $i < 25; $i++) {
                $docs[] = ['i' => $i];
            }
            $gl->documents->insertMany($coll, $docs)->await();

            $iter = $gl->documents->findCursor($coll, null, ['i' => 1], null, null, 7);
            $count = 0;
            foreach ($iter as $row) {
                $count++;
            }
            $this->assertSame(25, $count);
        } finally {
            $this->cleanup($coll, $gl);
            $gl->stop()->await();
        }
    }

    public function testPubSubRoundTrip(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $channel = $this->uniqCh();
        try {
            // Open a second connection for LISTEN to avoid self-listening
            // on the busy primary.
            $listenConn = $this->openExtraConnection($gl);
            try {
                $listener = $listenConn->listen($channel);

                // Publish from the primary connection in the background
                async(function () use ($gl, $channel) {
                    \Amp\delay(0.1);
                    $gl->publish($channel, 'hello')->await();
                });

                $received = null;
                $timeout = \Amp\async(function () {
                    \Amp\delay(2.0);
                    return null;
                });
                foreach ($listener as $notification) {
                    $received = $notification->payload;
                    $listener->unlisten();
                    break;
                }
                $this->assertSame('hello', $received);
            } finally {
                $listenConn->close();
            }
        } finally {
            $gl->stop()->await();
        }
    }

    public function testStreamReadWriteAck(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $stream = 'amp_str_' . bin2hex(random_bytes(4));
        try {
            $gl->streams->createGroup($stream, 'workers')->await();
            $id1 = $gl->streams->add($stream, ['task' => 'a'])->await();
            $id2 = $gl->streams->add($stream, ['task' => 'b'])->await();
            $this->assertGreaterThan(0, $id1);
            $this->assertGreaterThan($id1, $id2);

            $msgs = $gl->streams->read($stream, 'workers', 'w1', 10)->await();
            $this->assertCount(2, $msgs);
            $this->assertSame(['task' => 'a'], $msgs[0]['payload']);

            $acked = $gl->streams->ack($stream, 'workers', (int) $msgs[0]['id'])->await();
            $this->assertTrue($acked);
        } finally {
            $this->cleanup($stream, $gl);
            $this->cleanup($stream . '_groups', $gl);
            $this->cleanup($stream . '_pending', $gl);
            $gl->stop()->await();
        }
    }

    public function testSubprocessTerminatesOnStop(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $this->assertTrue($gl->isRunning());

        // Port should be occupied by the proxy
        $fp = @fsockopen('127.0.0.1', $port, $errno, $errstr, 1.0);
        $this->assertNotFalse($fp, 'proxy port should accept connections while running');
        if ($fp) {
            fclose($fp);
        }

        $gl->stop()->await();
        $this->assertFalse($gl->isRunning());

        // After stop, the port should eventually be released. Poll briefly.
        $freed = false;
        for ($i = 0; $i < 20; $i++) {
            usleep(100000);
            $fp = @fsockopen('127.0.0.1', $port, $errno, $errstr, 0.1);
            if ($fp === false) {
                $freed = true;
                break;
            }
            fclose($fp);
        }
        $this->assertTrue($freed, 'proxy port should be freed after stop()');
    }

    public function testSubscribeDeliversNotifications(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $channel = $this->uniqCh();
        try {
            // Second connection to LISTEN concurrently while primary publishes
            $listenConn = $this->openExtraConnection($gl);
            try {
                $received = [];
                $subscribeDone = async(function () use ($listenConn, $channel, &$received) {
                    $listener = $listenConn->listen($channel);
                    foreach ($listener as $notification) {
                        $received[] = $notification->payload;
                        if (count($received) >= 2) {
                            $listener->unlisten();
                            break;
                        }
                    }
                });

                \Amp\delay(0.1);
                $gl->publish($channel, 'first')->await();
                $gl->publish($channel, 'second')->await();

                $subscribeDone->await();
                $this->assertSame(['first', 'second'], $received);
            } finally {
                $listenConn->close();
            }
        } finally {
            $gl->stop()->await();
        }
    }

    public function testSearchRoundTrip(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $table = 'amp_search_' . bin2hex(random_bytes(4));
        try {
            $conn = $gl->connection();
            $conn->query("CREATE TABLE {$table} (id BIGSERIAL PRIMARY KEY, body TEXT)");
            $conn->execute(
                "INSERT INTO {$table} (body) VALUES (\$1), (\$2), (\$3)",
                ['the quick brown fox', 'postgres tuning guide', 'redis vs postgres']
            );

            $hits = $gl->search($table, 'body', 'postgres')->await();
            $this->assertGreaterThanOrEqual(2, count($hits));
        } finally {
            $this->cleanup($table, $gl);
            $gl->stop()->await();
        }
    }

    public function testSubprocessCleanupOnStopCancelsRunningProcess(): void
    {
        // stop() should terminate the Rust subprocess cleanly — this
        // exercises the cleanup path that also serves as the "Future
        // cancellation" recovery path (since our stop() is the async
        // termination primitive).
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $this->assertTrue($gl->isRunning());

        $ref = new ReflectionClass(GoldLapel::class);
        $procProp = $ref->getProperty('process');
        $procProp->setAccessible(true);
        $proc = $procProp->getValue($gl);
        $this->assertTrue(is_resource($proc));
        $pid = proc_get_status($proc)['pid'] ?? null;
        $this->assertNotNull($pid);

        $gl->stop()->await();
        $this->assertFalse($gl->isRunning());

        // Poll briefly — subprocess exit is async at the kernel level
        $dead = false;
        for ($i = 0; $i < 20; $i++) {
            usleep(100000);
            if (function_exists('posix_kill') && !@posix_kill($pid, 0)) {
                $dead = true;
                break;
            }
        }
        $this->assertTrue($dead, "subprocess pid {$pid} should exit after stop()");
    }

    public function testCachedConnectionHitsL1OnRepeatRead(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        $table = 'amp_cache_' . bin2hex(random_bytes(4));
        try {
            $conn = $gl->connection();
            $conn->query("CREATE TABLE {$table} (id INT PRIMARY KEY, v TEXT)");
            $conn->execute("INSERT INTO {$table} VALUES (\$1, \$2)", [1, 'alice']);
            $conn->execute("INSERT INTO {$table} VALUES (\$1, \$2)", [2, 'bob']);

            $cached = $gl->cached();
            $cache = $cached->getCache();
            $before = $cache->statsMisses;

            // First read — miss
            $rows1 = [];
            foreach ($cached->query("SELECT * FROM {$table} ORDER BY id") as $r) {
                $rows1[] = $r;
            }
            $this->assertCount(2, $rows1);

            // Second identical read — cache hit (if invalidation is connected)
            $hitsBefore = $cache->statsHits;
            $rows2 = [];
            foreach ($cached->query("SELECT * FROM {$table} ORDER BY id") as $r) {
                $rows2[] = $r;
            }
            $this->assertSame($rows1, $rows2);
            // Only assert on cache stats if the invalidation socket is up —
            // without it, put() short-circuits and every read is a miss.
            if ($cache->isConnected()) {
                $this->assertGreaterThan($hitsBefore, $cache->statsHits);
            }

            // Write via cached wrapper invalidates
            $cached->execute("UPDATE {$table} SET v = \$1 WHERE id = \$2", ['carol', 1]);
            $rows3 = [];
            foreach ($cached->query("SELECT * FROM {$table} ORDER BY id") as $r) {
                $rows3[] = $r;
            }
            $this->assertSame('carol', $rows3[0]['v']);
        } finally {
            $this->cleanup($table, $gl);
            $gl->stop()->await();
        }
    }

    public function testStartProxyOnlyAllowsDeferredConnect(): void
    {
        $port = $this->uniqPort();
        $gl = GoldLapel::startProxyOnly(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await();
        try {
            $this->assertNotNull($gl->url());
            $this->assertTrue($gl->isRunning());
            // connection() lazily opens on first use
            $conn = $gl->connection();
            $result = $conn->execute('SELECT 42 AS v');
            $this->assertSame(42, $result->fetchRow()['v']);
        } finally {
            $gl->stop()->await();
        }
    }
}
