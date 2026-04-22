<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Ddl;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end streams integration test — proxy-owned DDL (Phase 3).
 *
 * Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM — the
 * standardized integration-test convention shared across all Gold Lapel
 * wrappers. See tests/IntegrationGate.php. Also requires GOLDLAPEL_BINARY
 * pointing at a goldlapel binary with the DDL API.
 */
class StreamsIntegrationTest extends TestCase
{
    private static ?string $pgUrl = null;
    private static ?string $binary = null;

    public static function setUpBeforeClass(): void
    {
        // Evaluating the gate here surfaces the half-configured CI case
        // (GOLDLAPEL_INTEGRATION=1 set, GOLDLAPEL_TEST_UPSTREAM missing) as
        // a RuntimeException during PHPUnit setup — preventing false-green.
        self::$pgUrl = IntegrationGate::upstream();
        self::$binary = getenv('GOLDLAPEL_BINARY') ?: null;
    }

    protected function setUp(): void
    {
        if (self::$pgUrl === null) {
            $this->markTestSkipped(IntegrationGate::skipReason());
        }
        if (!self::$binary || !is_file(self::$binary)) {
            $this->markTestSkipped('Set GOLDLAPEL_BINARY to a goldlapel binary (v0.2+) to run this test.');
        }
        $pdo = @new \PDO(self::_toPdoDsn(self::$pgUrl));
        if (!$pdo) {
            $this->markTestSkipped('Postgres not reachable at ' . self::$pgUrl);
        }
    }

    public function testStreamAddCreatesPrefixedTable(): void
    {
        $port = 7700 + (int) (microtime(true) * 1000) % 100;
        $name = 'gl_php_int_stream_' . (int) (microtime(true) * 1000);
        // Proxy result_cache / consolidation / prepared_cache are intentionally
        // left on — the FOR UPDATE tx-poisoning bug that used to require
        // disabling them was fixed in goldlapel d77fe37 / 945d674 and has
        // regression coverage in tests/phase32_for_update_cache.rs.
        $gl = GoldLapel::start(self::$pgUrl, [
            'proxy_port' => $port,
            'silent' => true,
        ]);
        try {
            $gl->streamAdd($name, ['type' => 'click']);

            $direct = new \PDO(self::_toPdoDsn(self::$pgUrl));
            $stmt = $direct->prepare(
                "SELECT COUNT(*) FROM information_schema.tables "
                . "WHERE table_schema = '_goldlapel' AND table_name = :tbl"
            );
            $stmt->execute(['tbl' => "stream_{$name}"]);
            $this->assertSame(1, (int) $stmt->fetchColumn());

            $stmt2 = $direct->prepare(
                "SELECT COUNT(*) FROM information_schema.tables "
                . "WHERE table_schema = 'public' AND table_name = :tbl"
            );
            $stmt2->execute(['tbl' => $name]);
            $this->assertSame(0, (int) $stmt2->fetchColumn());
        } finally {
            $gl->stop();
        }
    }

    public function testSchemaMetaRowRecorded(): void
    {
        $port = 7800 + (int) (microtime(true) * 1000) % 100;
        $name = 'gl_php_int_meta_' . (int) (microtime(true) * 1000);
        // Proxy cache features left on — see testStreamAddCreatesPrefixedTable.
        $gl = GoldLapel::start(self::$pgUrl, [
            'proxy_port' => $port,
            'silent' => true,
        ]);
        try {
            $gl->streamAdd($name, ['type' => 'click']);
            $direct = new \PDO(self::_toPdoDsn(self::$pgUrl));
            $stmt = $direct->prepare(
                "SELECT family, name, schema_version FROM _goldlapel.schema_meta "
                . "WHERE family = 'stream' AND name = :n"
            );
            $stmt->execute(['n' => $name]);
            $row = $stmt->fetch(\PDO::FETCH_ASSOC);
            $this->assertNotFalse($row);
            $this->assertSame($name, $row['name']);
            $this->assertSame('v1', $row['schema_version']);
        } finally {
            $gl->stop();
        }
    }

    public function testRoundTrip(): void
    {
        $port = 7900 + (int) (microtime(true) * 1000) % 100;
        $name = 'gl_php_int_rt_' . (int) (microtime(true) * 1000);
        // Proxy cache features left on — see testStreamAddCreatesPrefixedTable.
        $gl = GoldLapel::start(self::$pgUrl, [
            'proxy_port' => $port,
            'silent' => true,
        ]);
        try {
            $gl->streamCreateGroup($name, 'workers');
            $gl->streamAdd($name, ['i' => 1]);
            $gl->streamAdd($name, ['i' => 2]);
            $msgs = $gl->streamRead($name, 'workers', 'c', 10);
            $this->assertCount(2, $msgs);
            $this->assertSame(['i' => 1], $msgs[0]['payload']);

            $ok = $gl->streamAck($name, 'workers', (int) $msgs[0]['id']);
            $this->assertTrue($ok);
            $again = $gl->streamAck($name, 'workers', (int) $msgs[0]['id']);
            $this->assertFalse($again);
        } finally {
            $gl->stop();
        }
    }

    /**
     * Dedicated regression: `SELECT ... FOR UPDATE` inside a PDO transaction
     * must acquire real row-level locks and commit cleanly, with every proxy
     * cache feature on. This is the exact pattern that used to silently
     * abort the transaction — PDO's DEALLOCATE-on-GC emitted by Q-mode would
     * reference a client-side statement name the proxy had renamed on the
     * wire, upstream errored "prepared statement does not exist", and inside
     * a transaction that error aborted the whole thing.
     *
     * Runs three begin/select-FOR-UPDATE/update/commit cycles back-to-back.
     * On broken builds, iter 1 fails with SQLSTATE[25P02] ("current
     * transaction is aborted"), iter 2+ fail with SQLSTATE[26000] ("prepared
     * statement does not exist"), and the final table state reflects none
     * of the intended updates.
     */
    public function testForUpdateInTransactionRoundTrips(): void
    {
        $port = 8000 + (int) (microtime(true) * 1000) % 100;
        $table = 'gl_php_fu_' . (int) (microtime(true) * 1000);
        // All three cache features on — this is the regression case.
        $gl = GoldLapel::start(self::$pgUrl, [
            'proxy_port' => $port,
            'silent' => true,
        ]);
        try {
            $dsn = self::_toPdoDsn(str_replace(
                self::_hostPort(self::$pgUrl),
                "127.0.0.1:{$port}",
                self::$pgUrl
            ));
            $pdo = new \PDO($dsn);
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            $pdo->exec(
                "DROP TABLE IF EXISTS {$table}; "
                . "CREATE TABLE {$table} (id INT PRIMARY KEY, val TEXT); "
                . "INSERT INTO {$table} VALUES (1,'a'),(2,'b'),(3,'c')"
            );

            for ($i = 1; $i <= 3; $i++) {
                $pdo->beginTransaction();
                try {
                    $sel = $pdo->prepare("SELECT id, val FROM {$table} WHERE id = ? FOR UPDATE");
                    $sel->execute([$i]);
                    $row = $sel->fetch(\PDO::FETCH_ASSOC);
                    $this->assertIsArray($row, "iter {$i}: FOR UPDATE must return a row");
                    $this->assertSame($i, (int) $row['id']);

                    $upd = $pdo->prepare("UPDATE {$table} SET val = ? WHERE id = ?");
                    $upd->execute(["upd_{$i}", $i]);
                    $this->assertSame(
                        1,
                        $upd->rowCount(),
                        "iter {$i}: UPDATE must affect exactly one row"
                    );

                    $pdo->commit();
                } catch (\Throwable $e) {
                    if ($pdo->inTransaction()) {
                        $pdo->rollBack();
                    }
                    throw $e;
                }
            }

            // Verify final state went through a direct (non-proxy) connection
            // so the assertion can't be satisfied by a cache hit.
            $direct = new \PDO(self::_toPdoDsn(self::$pgUrl));
            $rows = $direct
                ->query("SELECT id, val FROM {$table} ORDER BY id")
                ->fetchAll(\PDO::FETCH_ASSOC);
            $this->assertSame(
                [
                    ['id' => 1, 'val' => 'upd_1'],
                    ['id' => 2, 'val' => 'upd_2'],
                    ['id' => 3, 'val' => 'upd_3'],
                ],
                array_map(
                    fn($r) => ['id' => (int) $r['id'], 'val' => (string) $r['val']],
                    $rows
                ),
                'Every FOR UPDATE transaction must have committed its UPDATE'
            );

            // Clean up
            $direct->exec("DROP TABLE IF EXISTS {$table}");
        } finally {
            $gl->stop();
        }
    }

    private static function _hostPort(string $url): string
    {
        $parts = parse_url($url);
        $host = $parts['host'] ?? 'localhost';
        $port = $parts['port'] ?? 5432;
        return "{$host}:{$port}";
    }

    private static function _toPdoDsn(string $url): string
    {
        // postgres[ql]://user@host:port/db → pgsql:host=host;port=port;dbname=db;user=user
        $parts = parse_url($url);
        $host = $parts['host'] ?? 'localhost';
        $port = $parts['port'] ?? 5432;
        $db = ltrim($parts['path'] ?? '/postgres', '/');
        $user = $parts['user'] ?? null;
        $dsn = "pgsql:host={$host};port={$port};dbname={$db}";
        if ($user) $dsn .= ";user={$user}";
        return $dsn;
    }
}
