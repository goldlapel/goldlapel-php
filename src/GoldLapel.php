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
        'invalidation_port',
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
    private ?\PDO $pdo = null;

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

    public function pdo(): \PDO
    {
        if ($this->pdo === null) {
            throw new RuntimeException(
                "Not connected. Call startProxy() before using instance methods."
            );
        }
        return $this->pdo;
    }

    public function startProxy(): string
    {
        if ($this->process !== null && $this->isRunning()) {
            return $this->url;
        }

        $binary = self::findBinary();
        $cmd = array_merge(
            [$binary, '--upstream', $this->upstream, '--proxy-port', (string) $this->port],
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

                if (extension_loaded('pdo_pgsql')) {
                    $dsn = self::urlToPdoDsn($this->url);
                    $parsed = parse_url($this->url);
                    $user = isset($parsed['user']) ? rawurldecode($parsed['user']) : null;
                    $pass = isset($parsed['pass']) ? rawurldecode($parsed['pass']) : null;
                    $this->pdo = new \PDO($dsn, $user, $pass);
                }

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
        $this->pdo = null;

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

    // -- Instance methods (delegate to Utils with stored PDO) --

    // Document Store

    public function docInsert(string $collection, array $document): array
    {
        return Utils::docInsert($this->pdo(), $collection, $document);
    }

    public function docInsertMany(string $collection, array $documents): array
    {
        return Utils::docInsertMany($this->pdo(), $collection, $documents);
    }

    public function docFind(string $collection, ?array $filter = null, ?array $sort = null, ?int $limit = null, ?int $skip = null): array
    {
        return Utils::docFind($this->pdo(), $collection, $filter, $sort, $limit, $skip);
    }

    public function docFindOne(string $collection, ?array $filter = null): ?array
    {
        return Utils::docFindOne($this->pdo(), $collection, $filter);
    }

    public function docUpdate(string $collection, array $filter, array $update): int
    {
        return Utils::docUpdate($this->pdo(), $collection, $filter, $update);
    }

    public function docUpdateOne(string $collection, array $filter, array $update): int
    {
        return Utils::docUpdateOne($this->pdo(), $collection, $filter, $update);
    }

    public function docDelete(string $collection, array $filter): int
    {
        return Utils::docDelete($this->pdo(), $collection, $filter);
    }

    public function docDeleteOne(string $collection, array $filter): int
    {
        return Utils::docDeleteOne($this->pdo(), $collection, $filter);
    }

    public function docCount(string $collection, ?array $filter = null): int
    {
        return Utils::docCount($this->pdo(), $collection, $filter);
    }

    public function docCreateIndex(string $collection, ?array $keys = null): void
    {
        Utils::docCreateIndex($this->pdo(), $collection, $keys);
    }

    public function docAggregate(string $collection, array $pipeline): array
    {
        return Utils::docAggregate($this->pdo(), $collection, $pipeline);
    }

    public function docWatch(string $collection, ?callable $callback = null): void
    {
        Utils::docWatch($this->pdo(), $collection, $callback);
    }

    public function docUnwatch(string $collection): void
    {
        Utils::docUnwatch($this->pdo(), $collection);
    }

    public function docCreateTtlIndex(string $collection, int $expireAfterSeconds, string $field = 'created_at'): void
    {
        Utils::docCreateTtlIndex($this->pdo(), $collection, $expireAfterSeconds, $field);
    }

    public function docRemoveTtlIndex(string $collection): void
    {
        Utils::docRemoveTtlIndex($this->pdo(), $collection);
    }

    public function docCreateCapped(string $collection, int $maxDocuments): void
    {
        Utils::docCreateCapped($this->pdo(), $collection, $maxDocuments);
    }

    public function docRemoveCap(string $collection): void
    {
        Utils::docRemoveCap($this->pdo(), $collection);
    }

    // Search

    public function search(string $table, string|array $column, string $query, int $limit = 50, string $lang = 'english', bool $highlight = false): array
    {
        return Utils::search($this->pdo(), $table, $column, $query, $limit, $lang, $highlight);
    }

    public function searchFuzzy(string $table, string $column, string $query, int $limit = 50, float $threshold = 0.3): array
    {
        return Utils::searchFuzzy($this->pdo(), $table, $column, $query, $limit, $threshold);
    }

    public function searchPhonetic(string $table, string $column, string $query, int $limit = 50): array
    {
        return Utils::searchPhonetic($this->pdo(), $table, $column, $query, $limit);
    }

    public function similar(string $table, string $column, array $vector, int $limit = 10): array
    {
        return Utils::similar($this->pdo(), $table, $column, $vector, $limit);
    }

    public function suggest(string $table, string $column, string $prefix, int $limit = 10): array
    {
        return Utils::suggest($this->pdo(), $table, $column, $prefix, $limit);
    }

    public function facets(string $table, string $column, int $limit = 50, ?string $query = null, string|array|null $queryColumn = null, string $lang = 'english'): array
    {
        return Utils::facets($this->pdo(), $table, $column, $limit, $query, $queryColumn, $lang);
    }

    public function aggregate(string $table, string $column, string $func, ?string $groupBy = null, int $limit = 50): array
    {
        return Utils::aggregate($this->pdo(), $table, $column, $func, $groupBy, $limit);
    }

    public function createSearchConfig(string $name, string $copyFrom = 'english'): void
    {
        Utils::createSearchConfig($this->pdo(), $name, $copyFrom);
    }

    // Pub/Sub & Queue

    public function publish(string $channel, string $message): void
    {
        Utils::publish($this->pdo(), $channel, $message);
    }

    public function subscribe(string $channel, callable $callback): void
    {
        Utils::subscribe($this->pdo(), $channel, $callback);
    }

    public function enqueue(string $queueTable, array $payload): void
    {
        Utils::enqueue($this->pdo(), $queueTable, $payload);
    }

    public function dequeue(string $queueTable): ?array
    {
        return Utils::dequeue($this->pdo(), $queueTable);
    }

    // Counters

    public function incr(string $table, string $key, int $amount = 1): int
    {
        return Utils::incr($this->pdo(), $table, $key, $amount);
    }

    public function getCounter(string $table, string $key): int
    {
        return Utils::getCounter($this->pdo(), $table, $key);
    }

    // Hash

    public function hset(string $table, string $key, string $field, mixed $value): void
    {
        Utils::hset($this->pdo(), $table, $key, $field, $value);
    }

    public function hget(string $table, string $key, string $field): mixed
    {
        return Utils::hget($this->pdo(), $table, $key, $field);
    }

    public function hgetall(string $table, string $key): array
    {
        return Utils::hgetall($this->pdo(), $table, $key);
    }

    public function hdel(string $table, string $key, string $field): bool
    {
        return Utils::hdel($this->pdo(), $table, $key, $field);
    }

    // Sorted Sets

    public function zadd(string $table, string $member, float $score): void
    {
        Utils::zadd($this->pdo(), $table, $member, $score);
    }

    public function zincrby(string $table, string $member, float $amount = 1): float
    {
        return Utils::zincrby($this->pdo(), $table, $member, $amount);
    }

    public function zrange(string $table, int $start = 0, int $stop = 10, bool $desc = true): array
    {
        return Utils::zrange($this->pdo(), $table, $start, $stop, $desc);
    }

    public function zrank(string $table, string $member, bool $desc = true): ?int
    {
        return Utils::zrank($this->pdo(), $table, $member, $desc);
    }

    public function zscore(string $table, string $member): ?float
    {
        return Utils::zscore($this->pdo(), $table, $member);
    }

    public function zrem(string $table, string $member): bool
    {
        return Utils::zrem($this->pdo(), $table, $member);
    }

    // Geo

    public function georadius(string $table, string $geomColumn, float $lon, float $lat, float $radiusMeters, int $limit = 50): array
    {
        return Utils::georadius($this->pdo(), $table, $geomColumn, $lon, $lat, $radiusMeters, $limit);
    }

    public function geoadd(string $table, string $nameColumn, string $geomColumn, string $name, float $lon, float $lat): void
    {
        Utils::geoadd($this->pdo(), $table, $nameColumn, $geomColumn, $name, $lon, $lat);
    }

    public function geodist(string $table, string $geomColumn, string $nameColumn, string $nameA, string $nameB): ?float
    {
        return Utils::geodist($this->pdo(), $table, $geomColumn, $nameColumn, $nameA, $nameB);
    }

    // Misc

    public function countDistinct(string $table, string $column): int
    {
        return Utils::countDistinct($this->pdo(), $table, $column);
    }

    public function script(string $luaCode, mixed ...$args): ?string
    {
        return Utils::script($this->pdo(), $luaCode, ...$args);
    }

    // Streams

    public function streamAdd(string $stream, array $payload): int
    {
        return Utils::streamAdd($this->pdo(), $stream, $payload);
    }

    public function streamCreateGroup(string $stream, string $group): void
    {
        Utils::streamCreateGroup($this->pdo(), $stream, $group);
    }

    public function streamRead(string $stream, string $group, string $consumer, int $count = 1): array
    {
        return Utils::streamRead($this->pdo(), $stream, $group, $consumer, $count);
    }

    public function streamAck(string $stream, string $group, int $messageId): bool
    {
        return Utils::streamAck($this->pdo(), $stream, $group, $messageId);
    }

    public function streamClaim(string $stream, string $group, string $consumer, int $minIdleMs = 60000): array
    {
        return Utils::streamClaim($this->pdo(), $stream, $group, $consumer, $minIdleMs);
    }

    // Percolate

    public function percolateAdd(string $name, string $queryId, string $query, string $lang = 'english', ?array $metadata = null): void
    {
        Utils::percolateAdd($this->pdo(), $name, $queryId, $query, $lang, $metadata);
    }

    public function percolate(string $name, string $text, int $limit = 50, string $lang = 'english'): array
    {
        return Utils::percolate($this->pdo(), $name, $text, $limit, $lang);
    }

    public function percolateDelete(string $name, string $queryId): bool
    {
        return Utils::percolateDelete($this->pdo(), $name, $queryId);
    }

    // Debug

    public function analyze(string $text, string $lang = 'english'): array
    {
        return Utils::analyze($this->pdo(), $text, $lang);
    }

    public function explainScore(string $table, string $column, string $query, string $idColumn, mixed $idValue, string $lang = 'english'): ?array
    {
        return Utils::explainScore($this->pdo(), $table, $column, $query, $idColumn, $idValue, $lang);
    }

    // -- Static instance management --

    /**
     * Start the proxy and return a CachedPDO connection.
     *
     * Requires the pdo_pgsql extension. Use proxyUrl() if you only need the
     * connection string.
     *
     * @return CachedPDO
     */
    public static function start(string $upstream, ?int $port = null, array $config = [], array $extraArgs = []): CachedPDO
    {
        if (isset(self::$instances[$upstream]) && self::$instances[$upstream]->isRunning()) {
            $inst = self::$instances[$upstream];
            $url = $inst->url;
        } else {
            $instance = new self($upstream, $port, $config, $extraArgs);
            self::$instances[$upstream] = $instance;

            if (!self::$cleanupRegistered) {
                register_shutdown_function([self::class, 'cleanup']);
                self::$cleanupRegistered = true;
            }

            $url = $instance->startProxy();
            $inst = $instance;
        }

        if (!extension_loaded('pdo_pgsql')) {
            throw new RuntimeException(
                'No supported database driver found. ' .
                'Enable the pdo_pgsql extension or use proxyUrl() if you only need the connection string.'
            );
        }

        $dsn = self::urlToPdoDsn($url);
        $parsed = parse_url($url);
        $user = isset($parsed['user']) ? rawurldecode($parsed['user']) : null;
        $pass = isset($parsed['pass']) ? rawurldecode($parsed['pass']) : null;
        $pdo = new \PDO($dsn, $user, $pass);
        $invPort = isset($config['invalidation_port'])
            ? (int) $config['invalidation_port']
            : $inst->port + 2;
        return self::wrapPDO($pdo, $invPort);
    }

    /**
     * Start the proxy and return the proxy URL string without wrapping.
     */
    public static function startUrl(string $upstream, ?int $port = null, array $config = [], array $extraArgs = []): string
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
            if (empty(self::$instances)) {
                NativeCache::reset();
            }
            return;
        }

        foreach (self::$instances as $instance) {
            $instance->stopProxy();
        }
        self::$instances = [];
        NativeCache::reset();
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

    public static function wrapPDO(\PDO $pdo, ?int $invalidationPort = null): CachedPDO
    {
        if ($invalidationPort === null) {
            // Auto-detect from the most recently started instance
            $port = self::DEFAULT_PORT;
            if (!empty(self::$instances)) {
                $inst = array_values(self::$instances)[0];
                $port = $inst->port;
                $invalidationPort = isset($inst->config['invalidation_port'])
                    ? (int) $inst->config['invalidation_port']
                    : $port + 2;
            } else {
                $invalidationPort = $port + 2;
            }
        }

        $cache = NativeCache::getInstance();
        if (!$cache->isConnected()) {
            $cache->connectInvalidation($invalidationPort);
        }

        return new CachedPDO($pdo, $cache);
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

    /**
     * Convert a PostgreSQL connection URL to a PDO DSN string.
     *
     * Input:  postgresql://user:pass@localhost:7932/mydb?sslmode=require
     * Output: pgsql:host=localhost;port=7932;dbname=mydb;sslmode=require
     */
    private static function urlToPdoDsn(string $url): string
    {
        $parsed = parse_url($url);
        $params = [];

        if (isset($parsed['host'])) {
            $params[] = 'host=' . $parsed['host'];
        }
        if (isset($parsed['port'])) {
            $params[] = 'port=' . $parsed['port'];
        }
        if (isset($parsed['path']) && $parsed['path'] !== '/') {
            $params[] = 'dbname=' . ltrim($parsed['path'], '/');
        }
        if (isset($parsed['query'])) {
            parse_str($parsed['query'], $queryParams);
            foreach ($queryParams as $k => $v) {
                $params[] = $k . '=' . $v;
            }
        }

        return 'pgsql:' . implode(';', $params);
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
