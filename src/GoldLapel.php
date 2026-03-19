<?php

namespace GoldLapel;

use RuntimeException;

class GoldLapel
{
    const DEFAULT_PORT = 7932;
    const DEFAULT_DASHBOARD_PORT = 7933;
    const STARTUP_TIMEOUT = 10.0;
    const STARTUP_POLL_INTERVAL = 0.05;

    private const VALID_CONFIG_KEYS = [
        'mode', 'min_pattern_count', 'refresh_interval_secs', 'pattern_ttl_secs',
        'max_tables_per_view', 'max_columns_per_view', 'deep_pagination_threshold',
        'report_interval_secs', 'result_cache_size', 'batch_cache_size',
        'batch_cache_ttl_secs', 'pool_size', 'pool_timeout_secs',
        'pool_mode', 'mgmt_idle_timeout', 'fallback', 'read_after_write_secs',
        'n1_threshold', 'n1_window_ms', 'n1_cross_threshold',
        'tls_cert', 'tls_key', 'tls_client_ca', 'config', 'dashboard_port',
        'disable_matviews', 'disable_consolidation', 'disable_btree_indexes',
        'disable_trigram_indexes', 'disable_expression_indexes',
        'disable_partial_indexes', 'disable_rewrite', 'disable_prepared_cache',
        'disable_result_cache', 'disable_pool',
        'disable_n1', 'disable_n1_cross_connection', 'disable_shadow_mode',
        'enable_coalescing', 'replica', 'exclude_tables',
    ];

    private const BOOLEAN_KEYS = [
        'disable_matviews', 'disable_consolidation', 'disable_btree_indexes',
        'disable_trigram_indexes', 'disable_expression_indexes',
        'disable_partial_indexes', 'disable_rewrite', 'disable_prepared_cache',
        'disable_result_cache', 'disable_pool',
        'disable_n1', 'disable_n1_cross_connection', 'disable_shadow_mode',
        'enable_coalescing',
    ];

    private const LIST_KEYS = [
        'replica', 'exclude_tables',
    ];

    private string $upstream;
    private int $port;
    private int $dashboardPort;
    private array $config;
    private array $extraArgs;
    /** @var resource|null */
    private $process = null;
    private ?string $url = null;

    /** @var array<string, self> */
    private static array $instances = [];
    private static bool $cleanupRegistered = false;

    public function __construct(string $upstream, ?int $port = null, array $config = [], array $extraArgs = [])
    {
        $this->upstream = $upstream;
        $this->port = $port ?? self::DEFAULT_PORT;
        $this->dashboardPort = isset($config['dashboard_port']) ? (int) $config['dashboard_port'] : self::DEFAULT_DASHBOARD_PORT;
        $this->config = $config;
        $this->extraArgs = $extraArgs;
    }

    public static function configToArgs(array $config): array
    {
        if (empty($config)) {
            return [];
        }

        $validKeys = array_flip(self::VALID_CONFIG_KEYS);
        $booleanKeys = array_flip(self::BOOLEAN_KEYS);
        $listKeys = array_flip(self::LIST_KEYS);
        $args = [];

        foreach ($config as $key => $value) {
            if (!isset($validKeys[$key])) {
                throw new \InvalidArgumentException("Unknown config key: {$key}");
            }

            $flag = '--' . str_replace('_', '-', $key);

            if (isset($booleanKeys[$key])) {
                if (!is_bool($value)) {
                    throw new \TypeError("Config key '{$key}' must be a boolean, got " . gettype($value));
                }
                if ($value) {
                    $args[] = $flag;
                }
                continue;
            }

            if (isset($listKeys[$key])) {
                if (!is_array($value)) {
                    throw new \TypeError("Config key '{$key}' must be an array, got " . gettype($value));
                }
                foreach ($value as $item) {
                    $args[] = $flag;
                    $args[] = (string) $item;
                }
                continue;
            }

            $args[] = $flag;
            $args[] = (string) $value;
        }

        return $args;
    }

    public static function configKeys(): array
    {
        return self::VALID_CONFIG_KEYS;
    }

    public function startProxy(): string
    {
        if ($this->process !== null && $this->isRunning()) {
            return $this->url;
        }

        $binary = self::findBinary();
        $cmd = array_merge(
            [$binary, '--upstream', $this->upstream, '--port', (string) $this->port],
            self::configToArgs($this->config),
            $this->extraArgs
        );

        $nullDevice = PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null';
        $descriptors = [
            0 => ['file', $nullDevice, 'r'],
            1 => ['file', $nullDevice, 'w'],
            2 => ['pipe', 'w'],
        ];

        $env = getenv();
        if (!isset($env['GOLDLAPEL_CLIENT'])) {
            $env['GOLDLAPEL_CLIENT'] = 'php';
        }

        $pipes = [];
        $this->process = proc_open($cmd, $descriptors, $pipes, null, $env);

        if (!is_resource($this->process)) {
            throw new RuntimeException("Failed to start Gold Lapel process");
        }

        $stderr = $pipes[2];
        stream_set_blocking($stderr, false);
        $stderrOutput = '';

        $deadline = hrtime(true) + (int) (self::STARTUP_TIMEOUT * 1e9);

        while (hrtime(true) < $deadline) {
            $chunk = @fread($stderr, 65536);
            if ($chunk !== false && $chunk !== '') {
                $stderrOutput .= $chunk;
            }

            $status = proc_get_status($this->process);
            if (!$status['running']) {
                break;
            }

            if (self::waitForPort('127.0.0.1', $this->port, 0.5)) {
                fclose($stderr);
                $this->url = self::makeProxyUrl($this->upstream, $this->port);

                if ($this->dashboardPort > 0) {
                    echo "goldlapel → :{$this->port} (proxy) | http://127.0.0.1:{$this->dashboardPort} (dashboard)\n";
                } else {
                    echo "goldlapel → :{$this->port} (proxy)\n";
                }

                return $this->url;
            }
        }

        // Failure path — drain remaining stderr, terminate, throw
        $chunk = @fread($stderr, 65536);
        if ($chunk !== false && $chunk !== '') {
            $stderrOutput .= $chunk;
        }
        fclose($stderr);

        $this->terminate();

        throw new RuntimeException(
            "Gold Lapel failed to start on port {$this->port} within " . self::STARTUP_TIMEOUT . "s.\nstderr: {$stderrOutput}"
        );
    }

    public function stopProxy(): void
    {
        if ($this->process === null) {
            return;
        }

        $this->terminate();
        $this->url = null;
    }

    public function getUrl(): ?string
    {
        return $this->url;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getDashboardPort(): int
    {
        return $this->dashboardPort;
    }

    public function getDashboardUrl(): ?string
    {
        if ($this->dashboardPort > 0 && $this->isRunning()) {
            return "http://127.0.0.1:{$this->dashboardPort}";
        }
        return null;
    }

    public function isRunning(): bool
    {
        if ($this->process === null) {
            return false;
        }

        $status = proc_get_status($this->process);
        return $status['running'];
    }

    // -- Static instance management --

    public static function start(string $upstream, ?int $port = null, array $config = [], array $extraArgs = []): string
    {
        if (isset(self::$instances[$upstream]) && self::$instances[$upstream]->isRunning()) {
            return self::$instances[$upstream]->url;
        }

        $instance = new self($upstream, $port, $config, $extraArgs);
        self::$instances[$upstream] = $instance;

        if (!self::$cleanupRegistered) {
            register_shutdown_function([self::class, 'cleanup']);
            self::$cleanupRegistered = true;
        }

        return $instance->startProxy();
    }

    public static function stop(?string $upstream = null): void
    {
        if ($upstream !== null) {
            if (isset(self::$instances[$upstream])) {
                self::$instances[$upstream]->stopProxy();
                unset(self::$instances[$upstream]);
            }
            return;
        }

        foreach (self::$instances as $instance) {
            $instance->stopProxy();
        }
        self::$instances = [];
    }

    public static function proxyUrl(?string $upstream = null): ?string
    {
        if ($upstream !== null) {
            return (self::$instances[$upstream] ?? null)?->url;
        }

        if (empty(self::$instances)) {
            return null;
        }

        return array_values(self::$instances)[0]->url;
    }

    public static function dashboardUrl(?string $upstream = null): ?string
    {
        if ($upstream !== null) {
            return (self::$instances[$upstream] ?? null)?->getDashboardUrl();
        }

        if (empty(self::$instances)) {
            return null;
        }

        return array_values(self::$instances)[0]->getDashboardUrl();
    }

    public static function cleanup(): void
    {
        self::stop();
    }

    // -- Static helpers --

    public static function findBinary(): string
    {
        $envPath = getenv('GOLDLAPEL_BINARY');
        if ($envPath !== false && $envPath !== '') {
            if (!is_file($envPath)) {
                throw new RuntimeException(
                    "GOLDLAPEL_BINARY points to {$envPath} but file not found"
                );
            }
            return $envPath;
        }

        $os = match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Linux' => 'linux',
            'Windows' => 'windows',
            default => PHP_OS_FAMILY,
        };

        $arch = match (php_uname('m')) {
            'x86_64', 'amd64' => 'x86_64',
            'aarch64', 'arm64' => 'aarch64',
            default => php_uname('m'),
        };

        $binaryName = "goldlapel-{$os}-{$arch}";
        if ($os === 'linux' && self::isMusl($arch)) {
            $binaryName .= '-musl';
        }
        if ($os === 'windows') {
            $binaryName .= '.exe';
        }
        $bundled = __DIR__ . '/../bin/' . $binaryName;
        if (is_file($bundled)) {
            return realpath($bundled);
        }

        $pathBinary = self::which('goldlapel');
        if ($pathBinary !== null) {
            return $pathBinary;
        }

        throw new RuntimeException(
            "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " .
            "install the platform-specific package, or ensure 'goldlapel' is on PATH."
        );
    }

    public static function makeProxyUrl(string $upstream, int $port): string
    {
        $withPort = '/^(postgres(?:ql)?:\/\/(?:.*@)?)([^\/:?#]+):(\d+)(.*)$/';
        $noPort = '/^(postgres(?:ql)?:\/\/(?:.*@)?)([^\/:?#]+)(.*)$/';

        if (preg_match($withPort, $upstream, $m)) {
            return $m[1] . 'localhost:' . $port . $m[4];
        }

        if (preg_match($noPort, $upstream, $m)) {
            return $m[1] . 'localhost:' . $port . $m[3];
        }

        return 'localhost:' . $port;
    }

    public static function waitForPort(string $host, int $port, float $timeout): bool
    {
        $deadline = hrtime(true) + (int) ($timeout * 1e9);

        while (hrtime(true) < $deadline) {
            $fp = @fsockopen($host, $port, $errno, $errstr, 0.5);
            if ($fp !== false) {
                fclose($fp);
                return true;
            }
            usleep((int) (self::STARTUP_POLL_INTERVAL * 1e6));
        }

        return false;
    }

    // -- Private helpers --

    private function terminate(): void
    {
        if ($this->process === null) {
            return;
        }

        $status = proc_get_status($this->process);
        if ($status['running']) {
            proc_terminate($this->process, 15); // SIGTERM

            $deadline = hrtime(true) + (int) (5 * 1e9);
            while (hrtime(true) < $deadline) {
                $status = proc_get_status($this->process);
                if (!$status['running']) {
                    break;
                }
                usleep(50000);
            }

            $status = proc_get_status($this->process);
            if ($status['running']) {
                proc_terminate($this->process, 9); // SIGKILL
            }
        }

        proc_close($this->process);
        $this->process = null;
    }

    private static function isMusl(string $arch): bool
    {
        return file_exists("/lib/ld-musl-{$arch}.so.1");
    }

    private static function which(string $name): ?string
    {
        $path = getenv('PATH');
        if ($path === false) {
            return null;
        }

        $pathext = getenv('PATHEXT');
        $extensions = $pathext ? explode(';', $pathext) : [''];

        $dirs = explode(PATH_SEPARATOR, $path);
        foreach ($dirs as $dir) {
            foreach ($extensions as $ext) {
                $file = $dir . DIRECTORY_SEPARATOR . $name . $ext;
                if (is_file($file) && is_executable($file)) {
                    return $file;
                }
            }
        }

        return null;
    }
}
