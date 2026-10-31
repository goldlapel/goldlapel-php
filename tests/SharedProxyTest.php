<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use PHPUnit\Framework\TestCase;

/**
 * The shared proxy: one per machine and configuration, for PHP that starts
 * every request from nothing. Each "request" here is a PHP process of its
 * own, which carries nothing over, as under PHP-FPM; each test has its own
 * TMPDIR, so its own shared directory.
 *
 * The key and directory tests need no Postgres; the rest are gated on
 * GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM (see IntegrationGate)
 * and GOLDLAPEL_BINARY.
 */
class SharedProxyTest extends TestCase
{
    private string $tmp;
    /** @var list<resource> */
    private array $servers = [];

    protected function setUp(): void
    {
        if (PHP_OS_FAMILY === 'Windows' || !function_exists('posix_geteuid')) {
            $this->markTestSkipped('The shared proxy tests run on POSIX with the posix extension.');
        }
        $this->tmp = sys_get_temp_dir() . '/gl-shared-' . bin2hex(random_bytes(4));
        mkdir($this->tmp, 0700);
    }

    protected function tearDown(): void
    {
        foreach ($this->servers as $server) {
            proc_terminate($server);
            proc_close($server);
        }
        foreach ($this->instances() as $info) {
            $this->stopProxy((int) $info['pid']);
        }
        exec('rm -rf ' . escapeshellarg($this->tmp));
    }

    private function sharedDir(): string
    {
        return $this->tmp . '/goldlapel-' . posix_geteuid();
    }

    /** @return list<array<string, mixed>> what each live instance file says */
    private function instances(): array
    {
        $found = [];
        foreach (glob($this->sharedDir() . '/*.json') ?: [] as $file) {
            $info = json_decode((string) file_get_contents($file), true);
            if (is_array($info)) {
                $found[] = $info;
            }
        }
        return $found;
    }

    private function alive(int $pid): bool
    {
        return posix_kill($pid, 0);
    }

    private function stopProxy(int $pid): void
    {
        posix_kill($pid, SIGTERM);
        $deadline = microtime(true) + 10;
        while ($this->alive($pid) && microtime(true) < $deadline) {
            usleep(50000);
        }
    }

    private function waitUntil(callable $condition, float $seconds): bool
    {
        $deadline = microtime(true) + $seconds;
        while (microtime(true) < $deadline) {
            if ($condition()) {
                return true;
            }
            usleep(100000);
        }
        return $condition();
    }

    private function env(array $extra = []): array
    {
        return array_merge(getenv(), [
            'TMPDIR' => $this->tmp,
            // Off the network, as the proxy's own tests run it
            'GOLDLAPEL_DISABLE_TRIAL_REGISTER' => 'true',
            'GOLDLAPEL_HQ_BASE' => 'http://127.0.0.1:9',
            'GOLDLAPEL_TELEMETRY_URL' => 'http://127.0.0.1:9',
        ], $extra);
    }

    private function script(string $code): string
    {
        $file = $this->tmp . '/script-' . bin2hex(random_bytes(4)) . '.php';
        file_put_contents($file, "<?php\nrequire " . var_export(dirname(__DIR__) . '/vendor/autoload.php', true)
            . ";\nuse GoldLapel\\GoldLapel;\n" . $code);
        return $file;
    }

    /** @return resource */
    private function startPhp(string $code, array $env = [], ?array &$pipes = null)
    {
        $pipes = [];
        $process = proc_open([PHP_BINARY, $this->script($code)], [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, null, $this->env($env));
        $this->assertIsResource($process);
        return $process;
    }

    /** @return array{0: int, 1: string, 2: string} exit status, stdout, stderr */
    private function finishPhp($process, array $pipes): array
    {
        $out = (string) stream_get_contents($pipes[1]);
        $err = (string) stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        return [proc_close($process), $out, $err];
    }

    /** @return array{0: int, 1: string, 2: string} */
    private function php(string $code, array $env = []): array
    {
        $process = $this->startPhp($code, $env, $pipes);
        return $this->finishPhp($process, $pipes);
    }

    private const REQUEST = <<<'PHP'
$gl = GoldLapel::start(getenv('GOLDLAPEL_TEST_UPSTREAM'), ['shared' => true, 'silent' => true]);
echo json_encode(['port' => $gl->getProxyPort(), 'one' => (int) $gl->pdo()->query('SELECT 1')->fetchColumn()]);
PHP;

    /** One request against the shared proxy: its port, after a SELECT 1 through it. */
    private function request(array $env = []): int
    {
        [$status, $out, $err] = $this->php(self::REQUEST, $env);
        $this->assertSame(0, $status, "request failed: {$out}{$err}");
        $result = json_decode($out, true);
        $this->assertSame(1, $result['one']);
        return $result['port'];
    }

    private function requireIntegration(): void
    {
        if (IntegrationGate::upstream() === null) {
            $this->markTestSkipped(IntegrationGate::skipReason());
        }
        $binary = getenv('GOLDLAPEL_BINARY');
        if (!$binary || !is_file($binary)) {
            $this->markTestSkipped('Set GOLDLAPEL_BINARY to a goldlapel binary to run this test.');
        }
    }

    public function testTheKeyFollowsEverythingTheProxyReads(): void
    {
        $code = <<<'PHP'
$key = fn ($upstream = 'postgresql://u@h/db', $options = []) => basename(GoldLapel::sharedPrefix($upstream, $options));
$keys = ['base' => $key(), 'again' => $key()];
$keys['upstream'] = $key('postgresql://u@h/other');
$keys['option'] = $key('postgresql://u@h/db', ['config' => ['pool_size' => 5]]);
putenv('GOLDLAPEL_POOL_SIZE=5');
$keys['env'] = $key();
putenv('GOLDLAPEL_POOL_SIZE');
$app = getenv('TMPDIR') . '/app';
mkdir("$app/public", 0700, true);
chdir("$app/public");
$keys['cwd'] = $key();
file_put_contents("$app/.env", "GOLDLAPEL_POOL_SIZE=5\n");
$keys['dotenv'] = $key();
file_put_contents("$app/.env", "GOLDLAPEL_POOL_SIZE=6\n");
$keys['dotenv_changed'] = $key();
echo json_encode($keys);
PHP;
        [$status, $out, $err] = $this->php($code, ['GOLDLAPEL_BINARY' => PHP_BINARY]);
        $this->assertSame(0, $status, $out . $err);
        $keys = json_decode($out, true);
        $this->assertSame($keys['base'], $keys['again']);
        // Each input moves it; the walked-up .env (FPM starts in public/) counts
        $this->assertCount(count($keys) - 1, array_unique(array_values($keys)), $out);
    }

    public function testSilentAndSharedDontChangeTheKey(): void
    {
        // The constructor keys on the options less these two
        [$status, $out, $err] = $this->php(
            "\$gl = new GoldLapel('postgresql://u@h/db', ['silent' => true, 'shared' => true]);\n"
            . "\$keyOptions = (new ReflectionProperty(GoldLapel::class, 'keyOptions'))->getValue(\$gl);\n"
            . "echo json_encode(\$keyOptions);",
            ['GOLDLAPEL_BINARY' => PHP_BINARY],
        );
        $this->assertSame(0, $status, $out . $err);
        $this->assertSame([], json_decode($out, true));
    }

    public function testAnUnsafeDirectoryIsRefused(): void
    {
        mkdir($this->sharedDir(), 0755);
        chmod($this->sharedDir(), 0755);
        [, $out, $err] = $this->php('GoldLapel::sharedDir();');
        $this->assertStringContainsString("won't keep its shared proxy's files", $out . $err);

        rmdir($this->sharedDir());
        mkdir($this->tmp . '/elsewhere', 0700);
        symlink($this->tmp . '/elsewhere', $this->sharedDir());
        [, $out, $err] = $this->php('GoldLapel::sharedDir();');
        $this->assertStringContainsString("won't keep its shared proxy's files", $out . $err);
    }

    public function testRequestsShareOneProxyThatOutlivesThem(): void
    {
        $this->requireIntegration();
        $port = $this->request();
        $this->assertSame($port, $this->request());

        $instances = $this->instances();
        $this->assertCount(1, $instances);
        $this->assertSame($port, $instances[0]['proxy_port']);
        $this->assertTrue($this->alive((int) $instances[0]['pid']), 'the proxy left with the requests');
    }

    public function testStopLetsGoWithoutStoppingIt(): void
    {
        $this->requireIntegration();
        [$status, $out, $err] = $this->php(
            "\$gl = GoldLapel::start(getenv('GOLDLAPEL_TEST_UPSTREAM'), ['shared' => true, 'silent' => true]);\n"
            . "\$gl->stop();\necho json_encode(\$gl->isRunning());"
        );
        $this->assertSame(0, $status, $out . $err);
        $this->assertSame('false', $out);
        $this->assertTrue($this->alive((int) $this->instances()[0]['pid']));
    }

    public function testConcurrentColdStartsMakeOneProxy(): void
    {
        $this->requireIntegration();
        $running = [];
        for ($i = 0; $i < 8; $i++) {
            $process = $this->startPhp(self::REQUEST, [], $pipes);
            $running[] = [$process, $pipes];
        }
        $ports = [];
        $refused = 0;
        foreach ($running as [$process, $pipes]) {
            [$status, $out, $err] = $this->finishPhp($process, $pipes);
            if ($status !== 0) {
                // A request that found it still starting gives up after a moment
                $this->assertStringContainsString('still starting', $out . $err);
                $refused++;
                continue;
            }
            $ports[] = json_decode($out, true)['port'];
        }
        $this->assertNotEmpty($ports);
        $this->assertCount(1, array_unique($ports));
        $this->assertCount(1, $this->instances());
        if ($refused > 0) {
            // Ready for the next request
            $this->assertSame($ports[0], $this->request());
        }
    }

    public function testItIdlesOutOnceNoProcessUsesIt(): void
    {
        $this->requireIntegration();
        $idle = ['GOLDLAPEL_IDLE_EXIT_SECS' => '2'];

        // A process that holds it (a worker), connected or not, keeps it
        $holder = $this->startPhp(
            "GoldLapel::startProxyOnly(getenv('GOLDLAPEL_TEST_UPSTREAM'), ['shared' => true, 'silent' => true]);\nsleep(8);",
            $idle,
            $pipes,
        );
        $this->assertTrue($this->waitUntil(fn () => count($this->instances()) === 1, 15), 'never started');
        $pid = (int) $this->instances()[0]['pid'];
        sleep(4);
        $this->assertTrue($this->alive($pid), 'idled out under a holder');

        [$status, $out, $err] = $this->finishPhp($holder, $pipes);
        $this->assertSame(0, $status, $out . $err);
        $this->assertTrue($this->waitUntil(fn () => !$this->alive($pid), 15), 'never idled out');
        $this->assertSame([], $this->instances());
    }

    public function testACrashedProxyIsReplaced(): void
    {
        $this->requireIntegration();
        $this->request();
        $pid = (int) $this->instances()[0]['pid'];
        posix_kill($pid, SIGKILL);
        $this->waitUntil(fn () => !$this->alive($pid), 5);

        $this->request();
        $instances = $this->instances();
        $this->assertCount(1, $instances);
        $this->assertNotSame($pid, (int) $instances[0]['pid']);
    }

    public function testABusyExplicitPortSurfacesTheProxysMessage(): void
    {
        $this->requireIntegration();
        $busy = stream_socket_server('tcp://0.0.0.0:0');
        $port = (int) substr(strrchr(stream_socket_get_name($busy, false), ':'), 1);
        try {
            [$status, $out, $err] = $this->php(
                "GoldLapel::start(getenv('GOLDLAPEL_TEST_UPSTREAM'), ['shared' => true, 'silent' => true, 'proxy_port' => {$port}]);"
            );
        } finally {
            fclose($busy);
        }
        $this->assertNotSame(0, $status);
        $this->assertStringContainsString('exited with status 1', $out . $err);
        $this->assertStringContainsString("port {$port}, for the proxy, is already in use", $out . $err);
    }

    /**
     * PHP's built-in server starts each request from nothing, as PHP-FPM
     * does, and shares by default: every request uses one proxy.
     */
    public function testTheBuiltInServerSharesOneProxyAcrossRequests(): void
    {
        $this->requireIntegration();
        $router = $this->script(
            "\$gl = GoldLapel::start(getenv('GOLDLAPEL_TEST_UPSTREAM'), ['silent' => true]);\n"
            . "echo json_encode(['port' => \$gl->getProxyPort(), 'one' => (int) \$gl->pdo()->query('SELECT 1')->fetchColumn()]);"
        );
        $probe = stream_socket_server('tcp://127.0.0.1:0');
        $httpPort = (int) substr(strrchr(stream_socket_get_name($probe, false), ':'), 1);
        fclose($probe);
        $this->servers[] = proc_open(
            [PHP_BINARY, '-S', "127.0.0.1:{$httpPort}", $router],
            [1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']],
            $pipes,
            null,
            $this->env(),
        );
        $this->assertTrue($this->waitUntil(fn () => @fsockopen('127.0.0.1', $httpPort) !== false, 10), 'server never listened');

        $ports = [];
        $times = [];
        for ($i = 0; $i < 20; $i++) {
            $start = hrtime(true);
            $body = @file_get_contents("http://127.0.0.1:{$httpPort}/");
            $times[] = (hrtime(true) - $start) / 1e6;
            $result = json_decode((string) $body, true);
            $this->assertSame(1, $result['one'] ?? null, "request {$i}: {$body}");
            $ports[] = $result['port'];
        }
        $this->assertCount(1, array_unique($ports));
        $this->assertCount(1, $this->instances());
        $rest = array_slice($times, 1);
        sort($rest);
        fwrite(STDERR, sprintf(
            "\nbuilt-in server: first request %.0f ms (starts the proxy), then median %.1f ms, max %.1f ms over %d requests\n",
            $times[0], $rest[intdiv(count($rest), 2)], max($rest), count($rest),
        ));
    }
}
