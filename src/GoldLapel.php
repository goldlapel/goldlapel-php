<?php

namespace GoldLapel;

use RuntimeException;

/**
 * Gold Lapel PHP wrapper — factory API.
 *
 * Usage:
 *     $gl = GoldLapel::start('postgresql://user:pass@db/mydb', [
 *         'port' => 7932,
 *         'log_level' => 'info',
 *     ]);
 *     $pdo = new PDO($gl->url());
 *     $hits = $gl->search('articles', 'body', 'postgres tuning');
 *     $gl->docInsert('events', ['type' => 'signup']);
 *     $gl->using($pdo, function ($gl) {
 *         $gl->docInsert('events', ['type' => 'order.created']);
 *     });
 *     $gl->stop();
 */
class GoldLapel
{
    const DEFAULT_PORT = 7932;
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

    /** Connection set by using(); takes precedence over $pdo. */
    private ?\PDO $scopedConn = null;

    /** @var array<int, self> */
    private static array $liveInstances = [];
    private static bool $cleanupRegistered = false;

    /**
     * Private constructor — use GoldLapel::start() to create and start an instance.
     *
     * Internal users (and tests) can still construct directly.
     */
    public function __construct(string $upstream, ?int $port = null, array $config = [], array $extraArgs = [])
    {
        $this->upstream = $upstream;
        $this->port = $port ?? self::DEFAULT_PORT;
        $this->dashboardPort = array_key_exists('dashboard_port', $config)
            ? (int) $config['dashboard_port']
            : $this->port + 1;
        $this->config = $config;
        $this->extraArgs = $extraArgs;
    }

    /**
     * Factory — start a Gold Lapel proxy and return a ready-to-use instance.
     *
     * Options (all optional):
     *   - 'port' (int): proxy port (default 7932)
     *   - 'log_level' (string): 'trace'|'debug'|'info'|'warn'|'error' — translated to the proxy's -v/-vv/-vvv verbosity flag
     *   - 'config' (array): per-proxy config keys (see configKeys())
     *   - 'extra_args' (array): raw extra CLI flags
     *
     * The options array also accepts any of the keys from configKeys()
     * directly at the top level — those are merged into 'config'. This makes
     * `['port' => 7932, 'mode' => 'waiter']` work as well as
     * `['port' => 7932, 'config' => ['mode' => 'waiter']]`.
     *
     * Eagerly opens a PDO connection to the proxy; raises RuntimeException
     * if pdo_pgsql is not enabled.
     */
    public static function start(string $upstream, array $options = []): self
    {
        [$port, $config, $extraArgs] = self::parseStartOptions($options);

        $instance = new self($upstream, $port, $config, $extraArgs);
        // startProxyWithoutConnect() (invoked via startProxy) registers the
        // instance with cleanupAll as soon as the subprocess spawns, before
        // the PDO is opened — so a PDO failure here still leaves the
        // subprocess cleanable via __destruct or the shutdown hook.
        $instance->startProxy();

        return $instance;
    }

    /**
     * Low-level factory variant: start the proxy and return just the proxy URL.
     * Useful when you want to manage your own PDO connections, or in
     * environments (like Laravel's service provider) where PDOs are created
     * later by the framework.
     */
    public static function startProxyOnly(string $upstream, array $options = []): string
    {
        [$port, $config, $extraArgs] = self::parseStartOptions($options);

        $instance = new self($upstream, $port, $config, $extraArgs);
        $instance->startProxyWithoutConnect();

        return $instance->url;
    }

    private static function parseStartOptions(array $options): array
    {
        $port = isset($options['port']) ? (int) $options['port'] : null;

        // Accept either {'config' => [...]} or top-level config keys mixed
        // into options. 'port', 'log_level', 'extra_args', 'config' are
        // reserved option keys; everything else is treated as a config key.
        $config = $options['config'] ?? [];
        $extraArgs = $options['extra_args'] ?? [];

        $reserved = ['port', 'log_level', 'config', 'extra_args'];
        foreach ($options as $key => $value) {
            if (in_array($key, $reserved, true)) {
                continue;
            }
            // Implicit top-level config key
            $config[$key] = $value;
        }

        if (array_key_exists('log_level', $options) && $options['log_level'] !== null) {
            $verboseFlag = self::translateLogLevel($options['log_level']);
            if ($verboseFlag !== null) {
                $extraArgs[] = $verboseFlag;
            }
        }

        return [$port, $config, $extraArgs];
    }

    /**
     * Translate a log level string to the proxy's count-based verbosity flag.
     * The Rust binary exposes verbosity as -v / -vv / -vvv (count flag) rather
     * than --log-level <level>, so wrappers translate on the spawn side.
     *
     * @return string|null "-v"/"-vv"/"-vvv" for info/debug/trace, null for warn/error
     * @throws \InvalidArgumentException if the value is not a recognized level
     */
    private static function translateLogLevel($level): ?string
    {
        if (!is_string($level)) {
            throw new \InvalidArgumentException(
                'log_level must be a string (one of: trace, debug, info, warn, error)'
            );
        }
        switch (strtolower($level)) {
            case 'trace':
                return '-vvv';
            case 'debug':
                return '-vv';
            case 'info':
                return '-v';
            case 'warn':
            case 'warning':
            case 'error':
                return null;
            default:
                throw new \InvalidArgumentException(
                    'log_level must be one of: trace, debug, info, warn, error'
                );
        }
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

    // ------------------------------------------------------------------
    // Connection resolution
    // ------------------------------------------------------------------

    /**
     * The PDO this instance will use for wrapper method calls by default.
     * Throws if the proxy hasn't been started (internal PDO not opened).
     */
    public function pdo(): \PDO
    {
        $conn = $this->scopedConn ?? $this->pdo;
        if ($conn === null) {
            throw new RuntimeException(
                'Not connected. Call GoldLapel::start() before using instance methods.'
            );
        }
        return $conn;
    }

    /**
     * Resolve the connection to use for a single wrapper method call.
     *
     * Precedence: explicit $conn argument > using() scope > internal PDO.
     */
    private function resolveConn(?\PDO $conn): \PDO
    {
        if ($conn !== null) {
            return $conn;
        }
        if ($this->scopedConn !== null) {
            return $this->scopedConn;
        }
        if ($this->pdo !== null) {
            return $this->pdo;
        }
        throw new RuntimeException(
            'Not connected. Pass conn: or call GoldLapel::start() before using instance methods.'
        );
    }

    /**
     * Run $callback with $connection scoped as the default PDO for any
     * wrapper method called on this instance during the callback.
     *
     * The scope is restored in `finally`, so it survives exceptions.
     * Supports nesting — the previous scope is restored on exit.
     *
     * PHP does not have thread-local storage semantics in typical
     * synchronous usage; the scope is tracked on this instance. Concurrent
     * access via ext-pthreads or similar is not supported.
     *
     * Example:
     *     $gl->using($pdo, function ($gl) {
     *         $pdo->beginTransaction();
     *         $gl->docInsert('events', ['type' => 'order.created']);
     *         $pdo->commit();
     *     });
     */
    public function using(\PDO $connection, callable $callback): mixed
    {
        $previous = $this->scopedConn;
        $this->scopedConn = $connection;
        try {
            return $callback($this);
        } finally {
            $this->scopedConn = $previous;
        }
    }

    // ------------------------------------------------------------------
    // Proxy lifecycle
    // ------------------------------------------------------------------

    private function startProxy(): string
    {
        $url = $this->startProxyWithoutConnect();

        if (!extension_loaded('pdo_pgsql')) {
            // Partial success — keep the proxy running and let the caller
            // fetch the URL via url(), but raise so they know no PDO is
            // attached.
            throw new RuntimeException(
                'pdo_pgsql extension is not enabled. Install it, or use '
                . 'GoldLapel::startProxyOnly() if you only need the connection string.'
            );
        }

        $dsn = self::urlToPdoDsn($url);
        $parsed = parse_url($url);
        $user = isset($parsed['user']) ? rawurldecode($parsed['user']) : null;
        $pass = isset($parsed['pass']) ? rawurldecode($parsed['pass']) : null;
        $this->pdo = new \PDO($dsn, $user, $pass);

        return $url;
    }

    private function startProxyWithoutConnect(): string
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
            throw new RuntimeException('Failed to start Gold Lapel process');
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

                // Register the instance for global cleanup immediately so
                // that any subsequent init step (e.g. PDO construction in
                // startProxy()) can throw without leaking the subprocess.
                self::registerForCleanup($this);

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

    /**
     * Stop the proxy for this instance. Idempotent.
     */
    public function stop(): void
    {
        $this->pdo = null;
        $this->scopedConn = null;

        if ($this->process === null) {
            unset(self::$liveInstances[spl_object_id($this)]);
            return;
        }

        $this->terminate();
        $this->url = null;
        unset(self::$liveInstances[spl_object_id($this)]);

        if (empty(self::$liveInstances)) {
            NativeCache::reset();
        }
    }

    public function __destruct()
    {
        // Backup cleanup — the user should call stop() explicitly, but this
        // prevents orphan goldlapel processes when the instance falls out
        // of scope.
        if ($this->process !== null) {
            try {
                $this->terminate();
            } catch (\Throwable $e) {
                // Destructors must not throw.
            }
        }
    }

    /**
     * Proxy URL for use with PDO: `new PDO($gl->url())`. Note that PHP's PDO
     * DSN format is not a postgresql:// URL — this returns the
     * postgresql://... form, and internally we translate it to a pgsql:
     * DSN when we open our own connection.
     *
     * If the caller wants the PDO DSN directly, use pdoDsn().
     */
    public function url(): ?string
    {
        return $this->url;
    }

    /**
     * PDO-format DSN string (pgsql:host=...;port=...). Useful for
     * constructing your own PDO directly.
     */
    public function pdoDsn(): ?string
    {
        return $this->url !== null ? self::urlToPdoDsn($this->url) : null;
    }

    /**
     * Raw PDO user/password tuple parsed from the upstream URL, so callers
     * who want to new PDO() themselves can pass them alongside pdoDsn().
     *
     * @return array{0:?string,1:?string}
     */
    public function pdoCredentials(): array
    {
        if ($this->url === null) {
            return [null, null];
        }
        $parsed = parse_url($this->url);
        $user = isset($parsed['user']) ? rawurldecode($parsed['user']) : null;
        $pass = isset($parsed['pass']) ? rawurldecode($parsed['pass']) : null;
        return [$user, $pass];
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

    // ------------------------------------------------------------------
    // Global cleanup
    // ------------------------------------------------------------------

    /**
     * Register an instance with the global live-instance tracker and ensure
     * the one-time shutdown hook is installed. Called as soon as the
     * subprocess is known to be running, so any later init failure (PDO,
     * etc.) still results in the subprocess being cleaned up.
     */
    private static function registerForCleanup(self $instance): void
    {
        self::$liveInstances[spl_object_id($instance)] = $instance;
        if (!self::$cleanupRegistered) {
            register_shutdown_function([self::class, 'cleanupAll']);
            self::$cleanupRegistered = true;
        }
    }

    public static function cleanupAll(): void
    {
        foreach (self::$liveInstances as $instance) {
            try {
                $instance->stop();
            } catch (\Throwable $e) {
                // Shutdown-time errors should not abort cleanup.
            }
        }
        self::$liveInstances = [];
        NativeCache::reset();
    }

    // ------------------------------------------------------------------
    // Integration helper: wrap a PDO with the L1 cache
    // ------------------------------------------------------------------

    public function wrapPDO(\PDO $pdo, ?int $invalidationPort = null): CachedPDO
    {
        if ($invalidationPort === null) {
            $invalidationPort = isset($this->config['invalidation_port'])
                ? (int) $this->config['invalidation_port']
                : $this->port + 2;
        }

        return self::wrapPDOStatic($pdo, $invalidationPort);
    }

    /**
     * Low-level helper: wrap a PDO with the L1 cache, connecting the cache
     * to the given invalidation port. Used by the Laravel integration where
     * PDOs are constructed by the framework outside the factory lifecycle.
     */
    public static function wrapPDOStatic(\PDO $pdo, int $invalidationPort): CachedPDO
    {
        $cache = NativeCache::getInstance();
        if (!$cache->isConnected()) {
            $cache->connectInvalidation($invalidationPort);
        }

        return new CachedPDO($pdo, $cache);
    }

    /**
     * Convenience: return a CachedPDO wrapping this instance's internal PDO.
     * Useful when you want the L1 cache on the factory-managed connection.
     */
    public function cached(): CachedPDO
    {
        if ($this->pdo === null) {
            throw new RuntimeException(
                'Not connected. cached() requires the internal PDO opened by start().'
            );
        }
        return $this->wrapPDO($this->pdo);
    }

    // ------------------------------------------------------------------
    // Instance wrapper methods — all accept an optional `conn:` override.
    // ------------------------------------------------------------------

    // Document Store

    public function docInsert(string $collection, array $document, ?\PDO $conn = null): array
    {
        return Utils::docInsert($this->resolveConn($conn), $collection, $document);
    }

    public function docInsertMany(string $collection, array $documents, ?\PDO $conn = null): array
    {
        return Utils::docInsertMany($this->resolveConn($conn), $collection, $documents);
    }

    public function docFind(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        ?\PDO $conn = null,
    ): array {
        return Utils::docFind($this->resolveConn($conn), $collection, $filter, $sort, $limit, $skip);
    }

    public function docFindOne(string $collection, ?array $filter = null, ?\PDO $conn = null): ?array
    {
        return Utils::docFindOne($this->resolveConn($conn), $collection, $filter);
    }

    public function docUpdate(string $collection, array $filter, array $update, ?\PDO $conn = null): int
    {
        return Utils::docUpdate($this->resolveConn($conn), $collection, $filter, $update);
    }

    public function docUpdateOne(string $collection, array $filter, array $update, ?\PDO $conn = null): int
    {
        return Utils::docUpdateOne($this->resolveConn($conn), $collection, $filter, $update);
    }

    public function docDelete(string $collection, array $filter, ?\PDO $conn = null): int
    {
        return Utils::docDelete($this->resolveConn($conn), $collection, $filter);
    }

    public function docDeleteOne(string $collection, array $filter, ?\PDO $conn = null): int
    {
        return Utils::docDeleteOne($this->resolveConn($conn), $collection, $filter);
    }

    public function docCount(string $collection, ?array $filter = null, ?\PDO $conn = null): int
    {
        return Utils::docCount($this->resolveConn($conn), $collection, $filter);
    }

    public function docCreateIndex(string $collection, ?array $keys = null, ?\PDO $conn = null): void
    {
        Utils::docCreateIndex($this->resolveConn($conn), $collection, $keys);
    }

    public function docAggregate(string $collection, array $pipeline, ?\PDO $conn = null): array
    {
        return Utils::docAggregate($this->resolveConn($conn), $collection, $pipeline);
    }

    public function docWatch(string $collection, ?callable $callback = null, ?\PDO $conn = null): void
    {
        Utils::docWatch($this->resolveConn($conn), $collection, $callback);
    }

    public function docUnwatch(string $collection, ?\PDO $conn = null): void
    {
        Utils::docUnwatch($this->resolveConn($conn), $collection);
    }

    public function docCreateTtlIndex(
        string $collection,
        int $expireAfterSeconds,
        string $field = 'created_at',
        ?\PDO $conn = null,
    ): void {
        Utils::docCreateTtlIndex($this->resolveConn($conn), $collection, $expireAfterSeconds, $field);
    }

    public function docRemoveTtlIndex(string $collection, ?\PDO $conn = null): void
    {
        Utils::docRemoveTtlIndex($this->resolveConn($conn), $collection);
    }

    public function docCreateCollection(string $collection, bool $unlogged = false, ?\PDO $conn = null): void
    {
        Utils::docCreateCollection($this->resolveConn($conn), $collection, $unlogged);
    }

    public function docCreateCapped(string $collection, int $maxDocuments, ?\PDO $conn = null): void
    {
        Utils::docCreateCapped($this->resolveConn($conn), $collection, $maxDocuments);
    }

    public function docRemoveCap(string $collection, ?\PDO $conn = null): void
    {
        Utils::docRemoveCap($this->resolveConn($conn), $collection);
    }

    public function docFindCursor(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        int $batchSize = 100,
        ?\PDO $conn = null,
    ): \Generator {
        return Utils::docFindCursor(
            $this->resolveConn($conn),
            $collection,
            $filter,
            $sort,
            $limit,
            $skip,
            $batchSize,
        );
    }

    public function docFindOneAndUpdate(
        string $collection,
        array $filter,
        array $update,
        ?\PDO $conn = null,
    ): ?array {
        return Utils::docFindOneAndUpdate($this->resolveConn($conn), $collection, $filter, $update);
    }

    public function docFindOneAndDelete(string $collection, array $filter, ?\PDO $conn = null): ?array
    {
        return Utils::docFindOneAndDelete($this->resolveConn($conn), $collection, $filter);
    }

    public function docDistinct(
        string $collection,
        string $field,
        ?array $filter = null,
        ?\PDO $conn = null,
    ): array {
        return Utils::docDistinct($this->resolveConn($conn), $collection, $field, $filter);
    }

    // Search

    public function search(
        string $table,
        string|array $column,
        string $query,
        int $limit = 50,
        string $lang = 'english',
        bool $highlight = false,
        ?\PDO $conn = null,
    ): array {
        return Utils::search($this->resolveConn($conn), $table, $column, $query, $limit, $lang, $highlight);
    }

    public function searchFuzzy(
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        float $threshold = 0.3,
        ?\PDO $conn = null,
    ): array {
        return Utils::searchFuzzy($this->resolveConn($conn), $table, $column, $query, $limit, $threshold);
    }

    public function searchPhonetic(
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        ?\PDO $conn = null,
    ): array {
        return Utils::searchPhonetic($this->resolveConn($conn), $table, $column, $query, $limit);
    }

    public function similar(
        string $table,
        string $column,
        array $vector,
        int $limit = 10,
        ?\PDO $conn = null,
    ): array {
        return Utils::similar($this->resolveConn($conn), $table, $column, $vector, $limit);
    }

    public function suggest(
        string $table,
        string $column,
        string $prefix,
        int $limit = 10,
        ?\PDO $conn = null,
    ): array {
        return Utils::suggest($this->resolveConn($conn), $table, $column, $prefix, $limit);
    }

    public function facets(
        string $table,
        string $column,
        int $limit = 50,
        ?string $query = null,
        string|array|null $queryColumn = null,
        string $lang = 'english',
        ?\PDO $conn = null,
    ): array {
        return Utils::facets($this->resolveConn($conn), $table, $column, $limit, $query, $queryColumn, $lang);
    }

    public function aggregate(
        string $table,
        string $column,
        string $func,
        ?string $groupBy = null,
        int $limit = 50,
        ?\PDO $conn = null,
    ): array {
        return Utils::aggregate($this->resolveConn($conn), $table, $column, $func, $groupBy, $limit);
    }

    public function createSearchConfig(string $name, string $copyFrom = 'english', ?\PDO $conn = null): void
    {
        Utils::createSearchConfig($this->resolveConn($conn), $name, $copyFrom);
    }

    // Pub/Sub & Queue

    public function publish(string $channel, string $message, ?\PDO $conn = null): void
    {
        Utils::publish($this->resolveConn($conn), $channel, $message);
    }

    public function subscribe(string $channel, callable $callback, ?\PDO $conn = null): void
    {
        Utils::subscribe($this->resolveConn($conn), $channel, $callback);
    }

    public function enqueue(string $queueTable, array $payload, ?\PDO $conn = null): void
    {
        Utils::enqueue($this->resolveConn($conn), $queueTable, $payload);
    }

    public function dequeue(string $queueTable, ?\PDO $conn = null): ?array
    {
        return Utils::dequeue($this->resolveConn($conn), $queueTable);
    }

    // Counters

    public function incr(string $table, string $key, int $amount = 1, ?\PDO $conn = null): int
    {
        return Utils::incr($this->resolveConn($conn), $table, $key, $amount);
    }

    public function getCounter(string $table, string $key, ?\PDO $conn = null): int
    {
        return Utils::getCounter($this->resolveConn($conn), $table, $key);
    }

    // Hash

    public function hset(string $table, string $key, string $field, mixed $value, ?\PDO $conn = null): void
    {
        Utils::hset($this->resolveConn($conn), $table, $key, $field, $value);
    }

    public function hget(string $table, string $key, string $field, ?\PDO $conn = null): mixed
    {
        return Utils::hget($this->resolveConn($conn), $table, $key, $field);
    }

    public function hgetall(string $table, string $key, ?\PDO $conn = null): array
    {
        return Utils::hgetall($this->resolveConn($conn), $table, $key);
    }

    public function hdel(string $table, string $key, string $field, ?\PDO $conn = null): bool
    {
        return Utils::hdel($this->resolveConn($conn), $table, $key, $field);
    }

    // Sorted Sets

    public function zadd(string $table, string $member, float $score, ?\PDO $conn = null): void
    {
        Utils::zadd($this->resolveConn($conn), $table, $member, $score);
    }

    public function zincrby(string $table, string $member, float $amount = 1, ?\PDO $conn = null): float
    {
        return Utils::zincrby($this->resolveConn($conn), $table, $member, $amount);
    }

    public function zrange(
        string $table,
        int $start = 0,
        int $stop = 10,
        bool $desc = true,
        ?\PDO $conn = null,
    ): array {
        return Utils::zrange($this->resolveConn($conn), $table, $start, $stop, $desc);
    }

    public function zrank(string $table, string $member, bool $desc = true, ?\PDO $conn = null): ?int
    {
        return Utils::zrank($this->resolveConn($conn), $table, $member, $desc);
    }

    public function zscore(string $table, string $member, ?\PDO $conn = null): ?float
    {
        return Utils::zscore($this->resolveConn($conn), $table, $member);
    }

    public function zrem(string $table, string $member, ?\PDO $conn = null): bool
    {
        return Utils::zrem($this->resolveConn($conn), $table, $member);
    }

    // Geo

    public function georadius(
        string $table,
        string $geomColumn,
        float $lon,
        float $lat,
        float $radiusMeters,
        int $limit = 50,
        ?\PDO $conn = null,
    ): array {
        return Utils::georadius($this->resolveConn($conn), $table, $geomColumn, $lon, $lat, $radiusMeters, $limit);
    }

    public function geoadd(
        string $table,
        string $nameColumn,
        string $geomColumn,
        string $name,
        float $lon,
        float $lat,
        ?\PDO $conn = null,
    ): void {
        Utils::geoadd($this->resolveConn($conn), $table, $nameColumn, $geomColumn, $name, $lon, $lat);
    }

    public function geodist(
        string $table,
        string $geomColumn,
        string $nameColumn,
        string $nameA,
        string $nameB,
        ?\PDO $conn = null,
    ): ?float {
        return Utils::geodist($this->resolveConn($conn), $table, $geomColumn, $nameColumn, $nameA, $nameB);
    }

    // Misc

    public function countDistinct(string $table, string $column, ?\PDO $conn = null): int
    {
        return Utils::countDistinct($this->resolveConn($conn), $table, $column);
    }

    public function script(string $luaCode, mixed ...$args): ?string
    {
        // `script` takes variadic positional args for the script; use the
        // ambient connection (no conn: override). Callers who need per-call
        // conn control should fall back to Utils::script() directly.
        return Utils::script($this->resolveConn(null), $luaCode, ...$args);
    }

    // Streams

    public function streamAdd(string $stream, array $payload, ?\PDO $conn = null): int
    {
        return Utils::streamAdd($this->resolveConn($conn), $stream, $payload);
    }

    public function streamCreateGroup(string $stream, string $group, ?\PDO $conn = null): void
    {
        Utils::streamCreateGroup($this->resolveConn($conn), $stream, $group);
    }

    public function streamRead(
        string $stream,
        string $group,
        string $consumer,
        int $count = 1,
        ?\PDO $conn = null,
    ): array {
        return Utils::streamRead($this->resolveConn($conn), $stream, $group, $consumer, $count);
    }

    public function streamAck(string $stream, string $group, int $messageId, ?\PDO $conn = null): bool
    {
        return Utils::streamAck($this->resolveConn($conn), $stream, $group, $messageId);
    }

    public function streamClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
        ?\PDO $conn = null,
    ): array {
        return Utils::streamClaim($this->resolveConn($conn), $stream, $group, $consumer, $minIdleMs);
    }

    // Percolate

    public function percolateAdd(
        string $name,
        string $queryId,
        string $query,
        string $lang = 'english',
        ?array $metadata = null,
        ?\PDO $conn = null,
    ): void {
        Utils::percolateAdd($this->resolveConn($conn), $name, $queryId, $query, $lang, $metadata);
    }

    public function percolate(
        string $name,
        string $text,
        int $limit = 50,
        string $lang = 'english',
        ?\PDO $conn = null,
    ): array {
        return Utils::percolate($this->resolveConn($conn), $name, $text, $limit, $lang);
    }

    public function percolateDelete(string $name, string $queryId, ?\PDO $conn = null): bool
    {
        return Utils::percolateDelete($this->resolveConn($conn), $name, $queryId);
    }

    // Debug

    public function analyze(string $text, string $lang = 'english', ?\PDO $conn = null): array
    {
        return Utils::analyze($this->resolveConn($conn), $text, $lang);
    }

    public function explainScore(
        string $table,
        string $column,
        string $query,
        string $idColumn,
        mixed $idValue,
        string $lang = 'english',
        ?\PDO $conn = null,
    ): ?array {
        return Utils::explainScore(
            $this->resolveConn($conn),
            $table,
            $column,
            $query,
            $idColumn,
            $idValue,
            $lang,
        );
    }

    // ------------------------------------------------------------------
    // Static helpers — kept for binary/URL/port utilities
    // ------------------------------------------------------------------

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

    public static function urlToPdoDsn(string $url): string
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

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

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
