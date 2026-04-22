<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\Ddl;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Ddl — the DDL API client + per-session cache.
 *
 * Uses PHP's built-in `php -S` dev server as a fake dashboard (no third-party
 * test tooling). The server is spawned per test-class; each test resets
 * a shared response queue via a file on disk.
 *
 * Mirrors goldlapel-python/tests/test_ddl.py.
 */
class DdlTest extends TestCase
{
    private static ?\Closure $serverStop = null;
    private static int $port;
    private static string $stateFile;

    public static function setUpBeforeClass(): void
    {
        self::$stateFile = sys_get_temp_dir() . '/gl_php_ddl_state_' . getmypid() . '.json';
        self::_resetState();

        // Tiny router that reads the queue + records captures. The state file
        // path is baked in (via PHP string interpolation) because env vars
        // don't propagate across php -S request children.
        $routerFile = sys_get_temp_dir() . '/gl_php_ddl_router_' . getmypid() . '.php';
        $state = self::$stateFile;
        $router = "<?php\n"
            . '$state = ' . var_export($state, true) . ";\n"
            . <<<'PHP'
$s = json_decode(file_get_contents($state), true) ?: ['responses' => [], 'captured' => []];
$body = file_get_contents('php://input');
$s['captured'][] = [
    'path' => $_SERVER['REQUEST_URI'] ?? '',
    'headers' => [
        'x-gl-dashboard' => $_SERVER['HTTP_X_GL_DASHBOARD'] ?? null,
        'content-type' => $_SERVER['HTTP_CONTENT_TYPE'] ?? null,
    ],
    'body' => $body,
];
if (!empty($s['responses'])) {
    $resp = array_shift($s['responses']);
} else {
    $resp = [500, ['error' => 'no_response']];
}
file_put_contents($state, json_encode($s));
http_response_code((int) $resp[0]);
header('Content-Type: application/json');
echo json_encode($resp[1]);
PHP;
        file_put_contents($routerFile, $router);

        self::$port = self::_findFreePort();
        $cmd = sprintf(
            'php -S 127.0.0.1:%d %s > /dev/null 2>&1 & echo $!',
            self::$port,
            escapeshellarg($routerFile)
        );
        $pid = (int) trim(shell_exec($cmd));
        // Wait for listener
        for ($i = 0; $i < 20; $i++) {
            $c = @fsockopen('127.0.0.1', self::$port, $_, $_, 0.2);
            if ($c) { fclose($c); break; }
            usleep(100_000);
        }
        self::$serverStop = function () use ($pid, $routerFile) {
            posix_kill($pid, SIGTERM);
            @unlink($routerFile);
        };
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$serverStop) {
            (self::$serverStop)();
        }
        @unlink(self::$stateFile);
    }

    protected function setUp(): void
    {
        self::_resetState();
    }

    private static function _resetState(): void
    {
        file_put_contents(self::$stateFile, json_encode(['responses' => [], 'captured' => []]));
    }

    private static function _queue(int $status, array $body): void
    {
        $s = json_decode(file_get_contents(self::$stateFile), true);
        $s['responses'][] = [$status, $body];
        file_put_contents(self::$stateFile, json_encode($s));
    }

    private static function _captured(): array
    {
        $s = json_decode(file_get_contents(self::$stateFile), true);
        return $s['captured'] ?? [];
    }

    private static function _findFreePort(): int
    {
        $sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        socket_bind($sock, '127.0.0.1', 0);
        socket_getsockname($sock, $_, $port);
        socket_close($sock);
        return $port;
    }

    // ------ Tests ------

    public function testSupportedVersionStreamIsV1(): void
    {
        $this->assertSame('v1', Ddl::supportedVersion('stream'));
    }

    public function testHappyPathPostsCorrectBodyAndHeaders(): void
    {
        self::_queue(200, [
            'accepted' => true,
            'family' => 'stream',
            'schema_version' => 'v1',
            'tables' => ['main' => '_goldlapel.stream_events'],
            'query_patterns' => ['insert' => 'INSERT ...'],
        ]);
        $cache = [];
        $entry = Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
        $this->assertSame('_goldlapel.stream_events', $entry['tables']['main']);
        $this->assertSame('INSERT ...', $entry['query_patterns']['insert']);

        $caps = self::_captured();
        $this->assertCount(1, $caps);
        $this->assertSame('/api/ddl/stream/create', $caps[0]['path']);
        $this->assertSame('tok', $caps[0]['headers']['x-gl-dashboard']);
        $decoded = json_decode($caps[0]['body'], true);
        $this->assertSame('events', $decoded['name']);
        $this->assertSame('v1', $decoded['schema_version']);
    }

    public function testCacheHitDoesNotRepost(): void
    {
        self::_queue(200, [
            'tables' => ['main' => 'x'],
            'query_patterns' => ['insert' => 'X'],
        ]);
        $cache = [];
        $r1 = Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
        $r2 = Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
        $this->assertSame($r1, $r2);
        $this->assertCount(1, self::_captured());
    }

    public function testDifferentOwnersIsolated(): void
    {
        self::_queue(200, [
            'tables' => ['main' => '_goldlapel.stream_events'],
            'query_patterns' => ['insert' => 'X'],
        ]);
        self::_queue(200, [
            'tables' => ['main' => '_goldlapel.stream_events'],
            'query_patterns' => ['insert' => 'X'],
        ]);
        $cacheA = [];
        $cacheB = [];
        Ddl::fetchPatterns($cacheA, 'stream', 'events', self::$port, 'tok');
        Ddl::fetchPatterns($cacheB, 'stream', 'events', self::$port, 'tok');
        $this->assertCount(2, self::_captured(), 'isolated caches must each trigger a fetch');
    }

    public function testDifferentNamesMissCache(): void
    {
        self::_queue(200, [
            'tables' => ['main' => '_goldlapel.stream_events'],
            'query_patterns' => ['insert' => 'INSERT events'],
        ]);
        self::_queue(200, [
            'tables' => ['main' => '_goldlapel.stream_orders'],
            'query_patterns' => ['insert' => 'INSERT orders'],
        ]);
        $cache = [];
        Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
        Ddl::fetchPatterns($cache, 'stream', 'orders', self::$port, 'tok');
        $this->assertCount(2, self::_captured(), 'different names must each trigger a fetch');
    }

    public function testVersionMismatchRaisesActionable(): void
    {
        self::_queue(409, [
            'error' => 'version_mismatch',
            'detail' => 'wrapper requested v1; proxy speaks v2 — upgrade proxy',
        ]);
        $cache = [];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/schema version mismatch/');
        Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
    }

    public function testForbiddenRaisesTokenError(): void
    {
        self::_queue(403, ['error' => 'forbidden']);
        $cache = [];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/dashboard token/');
        Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
    }

    public function testMissingTokenRaisesBeforeHttp(): void
    {
        $cache = [];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/No dashboard token/');
        Ddl::fetchPatterns($cache, 'stream', 'events', 9999, null);
    }

    public function testMissingPortRaises(): void
    {
        $cache = [];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/No dashboard port/');
        Ddl::fetchPatterns($cache, 'stream', 'events', 0, 'tok');
    }

    public function testUnreachableRaisesActionable(): void
    {
        $cache = [];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/dashboard not reachable/');
        // Port 1 guaranteed unreachable
        Ddl::fetchPatterns($cache, 'stream', 'events', 1, 'tok');
    }

    public function testInvalidateDropsCache(): void
    {
        self::_queue(200, [
            'tables' => ['main' => 'x'],
            'query_patterns' => ['insert' => 'X'],
        ]);
        self::_queue(200, [
            'tables' => ['main' => 'x'],
            'query_patterns' => ['insert' => 'X'],
        ]);
        $cache = [];
        Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
        Ddl::invalidate($cache);
        Ddl::fetchPatterns($cache, 'stream', 'events', self::$port, 'tok');
        $this->assertCount(2, self::_captured());
    }

    public function testToPdoPlaceholders(): void
    {
        $this->assertSame(
            'INSERT INTO t (a,b) VALUES (?,?) RETURNING ?',
            Ddl::toPdoPlaceholders('INSERT INTO t (a,b) VALUES ($1,$2) RETURNING $3')
        );
        $this->assertSame(
            'SELECT ? FROM t',
            Ddl::toPdoPlaceholders('SELECT $10 FROM t')
        );
    }
}
