<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Ddl;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end streams integration test — proxy-owned DDL (Phase 3).
 *
 * Requires:
 *   - DATABASE_URL / GOLDLAPEL_TEST_PG_URL pointing at a reachable Postgres
 *   - GOLDLAPEL_BINARY pointing at a goldlapel binary with the DDL API
 *
 * Skipped if either is missing.
 */
class StreamsIntegrationTest extends TestCase
{
    private static ?string $pgUrl = null;
    private static ?string $binary = null;

    public static function setUpBeforeClass(): void
    {
        self::$pgUrl = getenv('GOLDLAPEL_TEST_PG_URL')
            ?: getenv('DATABASE_URL')
            ?: 'postgresql://sgibson@localhost:5432/postgres';
        self::$binary = getenv('GOLDLAPEL_BINARY') ?: null;
    }

    protected function setUp(): void
    {
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
        // Disable result cache + consolidation — streamRead uses FOR UPDATE
        // inside a transaction, which interacts poorly with the proxy's
        // in-memory result cache (pre-existing; this is not specific to the
        // proxy-owned-DDL migration).
        $gl = GoldLapel::start(self::$pgUrl, [
            'port' => $port,
            'silent' => true,
            'config' => [
                'disable_consolidation' => true,
                'disable_result_cache' => true,
                'disable_prepared_cache' => true,
            ],
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
        // Disable result cache + consolidation — streamRead uses FOR UPDATE
        // inside a transaction, which interacts poorly with the proxy's
        // in-memory result cache (pre-existing; this is not specific to the
        // proxy-owned-DDL migration).
        $gl = GoldLapel::start(self::$pgUrl, [
            'port' => $port,
            'silent' => true,
            'config' => [
                'disable_consolidation' => true,
                'disable_result_cache' => true,
                'disable_prepared_cache' => true,
            ],
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
        // Disable result cache + consolidation — streamRead uses FOR UPDATE
        // inside a transaction, which interacts poorly with the proxy's
        // in-memory result cache (pre-existing; this is not specific to the
        // proxy-owned-DDL migration).
        $gl = GoldLapel::start(self::$pgUrl, [
            'port' => $port,
            'silent' => true,
            'config' => [
                'disable_consolidation' => true,
                'disable_result_cache' => true,
                'disable_prepared_cache' => true,
            ],
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
