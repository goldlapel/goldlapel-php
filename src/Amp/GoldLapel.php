<?php

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\GoldLapel as SyncGoldLapel;
use RuntimeException;

use function Amp\async;

/**
 * Gold Lapel PHP wrapper — native async (Amp) factory API.
 *
 * Usage inside an Amp/Revolt event-loop context (any fiber):
 *
 *     use GoldLapel\Amp\GoldLapel;
 *
 *     $gl = GoldLapel::start('postgres://user:pass@host/db')->await();
 *     $hits = $gl->search('articles', 'body', 'postgres tuning')->await();
 *     $gl->docInsert('events', ['type' => 'signup'])->await();
 *     $gl->stop()->await();
 *
 *     // Scoped transactional coordination:
 *     $conn = $gl->connection();
 *     $gl->using($conn, function ($gl) {
 *         $gl->docInsert('events', ['type' => 'x'])->await();
 *         $gl->incr('counters', 'orders')->await();
 *     })->await();
 *
 * Every method that issues SQL returns Amp\Future<T>. Callers `->await()`
 * inside their own fiber to get the T. Amp's event loop is implicitly
 * used — if you're running this outside an event-loop context, await the
 * top-level Future or run inside EventLoop::queue().
 *
 * Subprocess lifecycle mirrors the sync factory: start() spawns the Rust
 * proxy via proc_open (sync, fast enough inside a fiber), opens an amphp
 * PostgresConnection to it, and registers for cleanup. stop() closes the
 * connection and terminates the subprocess.
 *
 * `using(PostgresLink, callable)` scopes a connection/transaction to the
 * current instance for the duration of the callback. Scope tracking is
 * instance-level (not fiber-local) — concurrent fibers mutating the same
 * GoldLapel instance's scope will race. For per-fiber isolation, construct
 * separate GoldLapel instances or pass the connection explicitly via the
 * trailing `$conn` argument on each method call.
 */
class GoldLapel
{
    const DEFAULT_PORT = SyncGoldLapel::DEFAULT_PORT;
    const STARTUP_TIMEOUT = SyncGoldLapel::STARTUP_TIMEOUT;

    private string $upstream;
    private int $port;
    private int $dashboardPort;
    private bool $silent;
    private array $config;
    private array $extraArgs;

    /** @var resource|null subprocess handle (via proc_open) */
    private $process = null;
    private ?string $url = null;
    private ?PostgresConnection $connection = null;
    /** Scope set by using(); takes precedence over $connection. */
    private ?PostgresExecutor $scopedConn = null;

    /** @var array<int, self> */
    private static array $liveInstances = [];
    private static bool $cleanupRegistered = false;

    public function __construct(
        string $upstream,
        ?int $port = null,
        array $config = [],
        array $extraArgs = [],
        bool $silent = false,
    ) {
        $this->upstream = $upstream;
        $this->port = $port ?? self::DEFAULT_PORT;
        $this->dashboardPort = array_key_exists('dashboard_port', $config)
            ? (int) $config['dashboard_port']
            : $this->port + 1;
        $this->silent = $silent;
        $this->config = $config;
        $this->extraArgs = $extraArgs;
    }

    /**
     * Factory — start a Gold Lapel proxy and open an async PostgresConnection.
     * Returns a Future<GoldLapel> resolved when the proxy is ready and
     * connected.
     *
     * Options match the sync factory (`port`, `log_level`, `config`,
     * `extra_args`, `silent`, plus top-level config keys).
     */
    public static function start(string $upstream, array $options = []): Future
    {
        return async(static function () use ($upstream, $options): self {
            $instance = self::startProxyInstance($upstream, $options);
            try {
                $instance->connect();
            } catch (\Throwable $e) {
                $instance->terminate();
                unset(self::$liveInstances[spl_object_id($instance)]);
                throw $e;
            }
            return $instance;
        });
    }

    /**
     * Lower-level factory: start the proxy and return an instance without
     * opening an async connection. Useful when callers want to manage their
     * own amphp connections (e.g. a connection pool).
     */
    public static function startProxyOnly(string $upstream, array $options = []): Future
    {
        return async(static function () use ($upstream, $options): self {
            return self::startProxyInstance($upstream, $options);
        });
    }

    private static function startProxyInstance(string $upstream, array $options): self
    {
        // Reuse sync option parser via reflection trick: rebuild the same
        // args. parseStartOptions is private on sync class, so we
        // re-derive here (short enough to duplicate vs. opening sync's
        // API further).
        [$port, $config, $extraArgs, $silent] = self::parseStartOptions($options);
        $instance = new self($upstream, $port, $config, $extraArgs, $silent);
        $instance->startSubprocess();
        return $instance;
    }

    private static function parseStartOptions(array $options): array
    {
        $port = isset($options['port']) ? (int) $options['port'] : null;
        $silent = !empty($options['silent']);
        $config = $options['config'] ?? [];
        $extraArgs = $options['extra_args'] ?? [];

        $reserved = ['port', 'log_level', 'config', 'extra_args', 'silent'];
        foreach ($options as $key => $value) {
            if (in_array($key, $reserved, true)) {
                continue;
            }
            $config[$key] = $value;
        }

        if (array_key_exists('log_level', $options) && $options['log_level'] !== null) {
            $flag = self::translateLogLevel($options['log_level']);
            if ($flag !== null) {
                $extraArgs[] = $flag;
            }
        }
        return [$port, $config, $extraArgs, $silent];
    }

    private static function translateLogLevel($level): ?string
    {
        if (!is_string($level)) {
            throw new \InvalidArgumentException(
                'log_level must be a string (one of: trace, debug, info, warn, error)'
            );
        }
        return match (strtolower($level)) {
            'trace' => '-vvv',
            'debug' => '-vv',
            'info' => '-v',
            'warn', 'warning', 'error' => null,
            default => throw new \InvalidArgumentException(
                'log_level must be one of: trace, debug, info, warn, error'
            ),
        };
    }

    // ------------------------------------------------------------------
    // Subprocess lifecycle
    // ------------------------------------------------------------------

    private function startSubprocess(): string
    {
        if ($this->process !== null && $this->isRunning()) {
            return $this->url;
        }
        $binary = SyncGoldLapel::findBinary();
        $cmd = array_merge(
            [$binary, '--upstream', $this->upstream, '--proxy-port', (string) $this->port],
            SyncGoldLapel::configToArgs($this->config),
            $this->extraArgs,
        );

        $nullDevice = PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null';
        $descriptors = [
            0 => ['file', $nullDevice, 'r'],
            1 => ['file', $nullDevice, 'w'],
            2 => ['pipe', 'w'],
        ];
        $env = getenv();
        if (!isset($env['GOLDLAPEL_CLIENT'])) {
            $env['GOLDLAPEL_CLIENT'] = 'php-amp';
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
            if (SyncGoldLapel::waitForPort('127.0.0.1', $this->port, 0.5)) {
                fclose($stderr);
                $this->url = SyncGoldLapel::makeProxyUrl($this->upstream, $this->port);
                self::registerForCleanup($this);
                $this->printBanner();
                return $this->url;
            }
        }
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

    private function printBanner(): void
    {
        if ($this->silent) {
            return;
        }
        $message = $this->dashboardPort > 0
            ? "goldlapel → :{$this->port} (proxy) | http://127.0.0.1:{$this->dashboardPort} (dashboard)\n"
            : "goldlapel → :{$this->port} (proxy)\n";
        $stream = defined('STDERR') ? STDERR : null;
        if (is_resource($stream)) {
            fwrite($stream, $message);
        }
    }

    /**
     * Open the amphp PostgresConnection to the proxy. Called by start()
     * after the subprocess is confirmed listening. Exposed so callers of
     * startProxyOnly() can defer connection opening until inside their
     * own event-loop context.
     */
    public function connect(): void
    {
        if ($this->connection !== null) {
            return;
        }
        if ($this->url === null) {
            throw new RuntimeException('Proxy is not running (url is null)');
        }
        $config = PostgresConfig::fromString(self::urlToAmphpConnString($this->url));
        $this->connection = \Amp\Postgres\connect($config);
    }

    /**
     * Stop the proxy for this instance. Idempotent.
     * Returns Future<void>.
     */
    public function stop(): Future
    {
        return async(function (): void {
            if ($this->connection !== null) {
                try {
                    $this->connection->close();
                } catch (\Throwable $e) {
                    // best-effort
                }
                $this->connection = null;
            }
            $this->scopedConn = null;
            if ($this->process === null) {
                unset(self::$liveInstances[spl_object_id($this)]);
                return;
            }
            $this->terminate();
            $this->url = null;
            unset(self::$liveInstances[spl_object_id($this)]);
        });
    }

    public function __destruct()
    {
        // Backup cleanup path if the user forgot stop(). Async cleanup
        // isn't appropriate from a destructor — just terminate the
        // subprocess synchronously.
        if ($this->process !== null) {
            try {
                $this->terminate();
            } catch (\Throwable $e) {
                // destructors must not throw
            }
        }
    }

    private function terminate(): void
    {
        if ($this->process === null) {
            return;
        }
        $status = proc_get_status($this->process);
        if ($status['running']) {
            proc_terminate($this->process, 15);
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
                proc_terminate($this->process, 9);
            }
        }
        proc_close($this->process);
        $this->process = null;
    }

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
                $instance->terminate();
            } catch (\Throwable $e) {
                // shutdown-time errors shouldn't abort cleanup
            }
        }
        self::$liveInstances = [];
    }

    public function isRunning(): bool
    {
        if ($this->process === null) {
            return false;
        }
        $status = proc_get_status($this->process);
        if (!$status['running']) {
            return false;
        }
        if (isset($status['exitcode']) && $status['exitcode'] !== -1) {
            return false;
        }
        if (function_exists('posix_kill') && isset($status['pid'])) {
            $pid = (int) $status['pid'];
            if ($pid > 0 && !@posix_kill($pid, 0)) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    public function url(): ?string
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

    /**
     * Return the underlying async connection. Opens one if startProxyOnly()
     * was used and connect() hasn't been called yet.
     */
    public function connection(): PostgresConnection
    {
        if ($this->connection === null) {
            $this->connect();
        }
        if ($this->connection === null) {
            throw new RuntimeException(
                'Not connected. Call connect() before using connection().'
            );
        }
        return $this->connection;
    }

    /**
     * Return a CachedConnection wrapping the given executor. Uses the
     * shared NativeCache (same instance the sync CachedPDO talks to), so
     * writes through either path invalidate the other's cache entries.
     *
     * Pass $invalidationPort to override the derived port (default is
     * proxy port + 2, or the `invalidation_port` config key).
     */
    public function wrapCached(
        PostgresExecutor $conn,
        ?int $invalidationPort = null,
    ): CachedConnection {
        if ($invalidationPort === null) {
            $invalidationPort = isset($this->config['invalidation_port'])
                ? (int) $this->config['invalidation_port']
                : $this->port + 2;
        }
        $cache = \GoldLapel\NativeCache::getInstance();
        if (!$cache->isConnected()) {
            $cache->connectInvalidation($invalidationPort);
        }
        return new CachedConnection($conn, $cache);
    }

    /**
     * Convenience: return a CachedConnection wrapping this instance's
     * async connection. Shortcut for wrapCached($this->connection()).
     */
    public function cached(): CachedConnection
    {
        return $this->wrapCached($this->connection());
    }

    private function resolveConn(?PostgresExecutor $conn): PostgresExecutor
    {
        if ($conn !== null) {
            return $conn;
        }
        if ($this->scopedConn !== null) {
            return $this->scopedConn;
        }
        if ($this->connection !== null) {
            return $this->connection;
        }
        throw new RuntimeException(
            'Not connected. Pass $conn or call connect() before using instance methods.'
        );
    }

    /**
     * Convert a `postgres://user:pass@host:port/db` URL to the
     * space-separated `host=... port=... user=... password=... dbname=...`
     * keyword form that Amp\Postgres\PostgresConfig::fromString() expects.
     * amphp does NOT accept URL form; its parser errors with "Trailing
     * characters in connection string".
     */
    private static function urlToAmphpConnString(string $url): string
    {
        $parsed = parse_url($url);
        if ($parsed === false || !isset($parsed['host'])) {
            throw new RuntimeException("Cannot parse proxy URL: {$url}");
        }
        $parts = ['host=' . $parsed['host']];
        if (isset($parsed['port'])) {
            $parts[] = 'port=' . (int) $parsed['port'];
        }
        if (isset($parsed['user']) && $parsed['user'] !== '') {
            $user = rawurldecode($parsed['user']);
            $parts[] = "user='" . addslashes($user) . "'";
        }
        if (isset($parsed['pass']) && $parsed['pass'] !== '') {
            $pass = rawurldecode($parsed['pass']);
            $parts[] = "password='" . addslashes($pass) . "'";
        }
        if (isset($parsed['path']) && $parsed['path'] !== '' && $parsed['path'] !== '/') {
            $db = ltrim($parsed['path'], '/');
            $parts[] = "dbname='" . addslashes($db) . "'";
        }
        return implode(' ', $parts);
    }

    // ------------------------------------------------------------------
    // Scoping
    // ------------------------------------------------------------------

    /**
     * Run $callback with $connection scoped as the default executor for any
     * async wrapper method called on this instance during the callback.
     * The scope is restored in `finally`, so it survives exceptions.
     *
     * Pass a PostgresConnection, a PostgresTransaction, or any
     * PostgresExecutor — transactions are the common case:
     *
     *     $tx = $gl->connection()->beginTransaction();
     *     $gl->using($tx, function ($gl) {
     *         $gl->docInsert('events', ['type' => 'x'])->await();
     *         $gl->incr('counters', 'orders')->await();
     *     })->await();
     *     $tx->commit();
     *
     * Returns Future<mixed> — the callback's return value.
     *
     * The callback is invoked synchronously inside this fiber; if it
     * returns a Future, that Future is awaited before the scope is
     * restored, so callers can safely return `->await()`ed values.
     */
    public function using(PostgresExecutor $connection, callable $callback): Future
    {
        return async(function () use ($connection, $callback) {
            $previous = $this->scopedConn;
            $this->scopedConn = $connection;
            try {
                $result = $callback($this);
                if ($result instanceof Future) {
                    $result = $result->await();
                }
                return $result;
            } finally {
                $this->scopedConn = $previous;
            }
        });
    }

    // ------------------------------------------------------------------
    // Instance method surface — each returns Future<T> so callers
    // `->await()`. SQL work happens on the resolved executor.
    // ------------------------------------------------------------------

    // Document Store

    public function docInsert(string $collection, array $document, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docInsert($this->resolveConn($conn), $collection, $document));
    }

    public function docInsertMany(string $collection, array $documents, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docInsertMany($this->resolveConn($conn), $collection, $documents));
    }

    public function docFind(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::docFind($this->resolveConn($conn), $collection, $filter, $sort, $limit, $skip));
    }

    public function docFindOne(string $collection, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docFindOne($this->resolveConn($conn), $collection, $filter));
    }

    public function docUpdate(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docUpdate($this->resolveConn($conn), $collection, $filter, $update));
    }

    public function docUpdateOne(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docUpdateOne($this->resolveConn($conn), $collection, $filter, $update));
    }

    public function docDelete(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docDelete($this->resolveConn($conn), $collection, $filter));
    }

    public function docDeleteOne(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docDeleteOne($this->resolveConn($conn), $collection, $filter));
    }

    public function docCount(string $collection, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docCount($this->resolveConn($conn), $collection, $filter));
    }

    public function docCreateIndex(string $collection, ?array $keys = null, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docCreateIndex($this->resolveConn($conn), $collection, $keys));
    }

    public function docAggregate(string $collection, array $pipeline, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docAggregate($this->resolveConn($conn), $collection, $pipeline));
    }

    /**
     * Install the change-stream trigger and (optionally) run a long-lived
     * listener. If $callback is non-null, the Future blocks until the
     * listener stops; this requires a full PostgresConnection as the
     * executor.
     */
    public function docWatch(string $collection, ?callable $callback = null, ?PostgresExecutor $conn = null): Future
    {
        return async(function () use ($collection, $callback, $conn) {
            $c = $this->resolveConn($conn);
            if (!$c instanceof PostgresConnection) {
                throw new \InvalidArgumentException(
                    'docWatch requires a PostgresConnection (LISTEN is not supported on transactions).'
                );
            }
            Utils::docWatch($c, $collection, $callback);
        });
    }

    public function docUnwatch(string $collection, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docUnwatch($this->resolveConn($conn), $collection));
    }

    public function docCreateTtlIndex(
        string $collection,
        int $expireAfterSeconds,
        string $field = 'created_at',
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::docCreateTtlIndex($this->resolveConn($conn), $collection, $expireAfterSeconds, $field));
    }

    public function docRemoveTtlIndex(string $collection, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docRemoveTtlIndex($this->resolveConn($conn), $collection));
    }

    public function docCreateCollection(string $collection, bool $unlogged = false, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docCreateCollection($this->resolveConn($conn), $collection, $unlogged));
    }

    public function docCreateCapped(string $collection, int $maxDocuments, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docCreateCapped($this->resolveConn($conn), $collection, $maxDocuments));
    }

    public function docRemoveCap(string $collection, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docRemoveCap($this->resolveConn($conn), $collection));
    }

    /**
     * Returns an Amp\Pipeline\ConcurrentIterator<array> — iterate with
     * foreach inside a fiber. The call itself is synchronous (no Future
     * wrap) because the iterator is the awaitable. Requires a full
     * PostgresConnection (cursor needs its own transaction).
     */
    public function docFindCursor(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        int $batchSize = 100,
        ?PostgresExecutor $conn = null,
    ): \Amp\Pipeline\ConcurrentIterator {
        $c = $this->resolveConn($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'docFindCursor requires a PostgresConnection (cursor-in-transaction).'
            );
        }
        return Utils::docFindCursor($c, $collection, $filter, $sort, $limit, $skip, $batchSize);
    }

    public function docFindOneAndUpdate(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docFindOneAndUpdate($this->resolveConn($conn), $collection, $filter, $update));
    }

    public function docFindOneAndDelete(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docFindOneAndDelete($this->resolveConn($conn), $collection, $filter));
    }

    public function docDistinct(string $collection, string $field, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::docDistinct($this->resolveConn($conn), $collection, $field, $filter));
    }

    // Search

    public function search(
        string $table,
        string|array $column,
        string $query,
        int $limit = 50,
        string $lang = 'english',
        bool $highlight = false,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::search($this->resolveConn($conn), $table, $column, $query, $limit, $lang, $highlight));
    }

    public function searchFuzzy(
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        float $threshold = 0.3,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::searchFuzzy($this->resolveConn($conn), $table, $column, $query, $limit, $threshold));
    }

    public function searchPhonetic(
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::searchPhonetic($this->resolveConn($conn), $table, $column, $query, $limit));
    }

    public function similar(
        string $table,
        string $column,
        array $vector,
        int $limit = 10,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::similar($this->resolveConn($conn), $table, $column, $vector, $limit));
    }

    public function suggest(
        string $table,
        string $column,
        string $prefix,
        int $limit = 10,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::suggest($this->resolveConn($conn), $table, $column, $prefix, $limit));
    }

    public function facets(
        string $table,
        string $column,
        int $limit = 50,
        ?string $query = null,
        string|array|null $queryColumn = null,
        string $lang = 'english',
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::facets($this->resolveConn($conn), $table, $column, $limit, $query, $queryColumn, $lang));
    }

    public function aggregate(
        string $table,
        string $column,
        string $func,
        ?string $groupBy = null,
        int $limit = 50,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::aggregate($this->resolveConn($conn), $table, $column, $func, $groupBy, $limit));
    }

    public function createSearchConfig(string $name, string $copyFrom = 'english', ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::createSearchConfig($this->resolveConn($conn), $name, $copyFrom));
    }

    // Pub/Sub & Queue

    public function publish(string $channel, string $message, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::publish($this->resolveConn($conn), $channel, $message));
    }

    public function subscribe(string $channel, callable $callback, ?PostgresExecutor $conn = null): Future
    {
        return async(function () use ($channel, $callback, $conn) {
            $c = $this->resolveConn($conn);
            if (!$c instanceof PostgresConnection) {
                throw new \InvalidArgumentException(
                    'subscribe requires a PostgresConnection (LISTEN is not supported on transactions).'
                );
            }
            Utils::subscribe($c, $channel, $callback);
        });
    }

    public function enqueue(string $queueTable, array $payload, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::enqueue($this->resolveConn($conn), $queueTable, $payload));
    }

    public function dequeue(string $queueTable, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::dequeue($this->resolveConn($conn), $queueTable));
    }

    // Counters

    public function incr(string $table, string $key, int $amount = 1, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::incr($this->resolveConn($conn), $table, $key, $amount));
    }

    public function getCounter(string $table, string $key, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::getCounter($this->resolveConn($conn), $table, $key));
    }

    // Hash

    public function hset(string $table, string $key, string $field, mixed $value, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::hset($this->resolveConn($conn), $table, $key, $field, $value));
    }

    public function hget(string $table, string $key, string $field, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::hget($this->resolveConn($conn), $table, $key, $field));
    }

    public function hgetall(string $table, string $key, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::hgetall($this->resolveConn($conn), $table, $key));
    }

    public function hdel(string $table, string $key, string $field, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::hdel($this->resolveConn($conn), $table, $key, $field));
    }

    // Sorted Sets

    public function zadd(string $table, string $member, float $score, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::zadd($this->resolveConn($conn), $table, $member, $score));
    }

    public function zincrby(string $table, string $member, float $amount = 1, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::zincrby($this->resolveConn($conn), $table, $member, $amount));
    }

    public function zrange(string $table, int $start = 0, int $stop = 10, bool $desc = true, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::zrange($this->resolveConn($conn), $table, $start, $stop, $desc));
    }

    public function zrank(string $table, string $member, bool $desc = true, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::zrank($this->resolveConn($conn), $table, $member, $desc));
    }

    public function zscore(string $table, string $member, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::zscore($this->resolveConn($conn), $table, $member));
    }

    public function zrem(string $table, string $member, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::zrem($this->resolveConn($conn), $table, $member));
    }

    // Geo

    public function georadius(
        string $table,
        string $geomColumn,
        float $lon,
        float $lat,
        float $radiusMeters,
        int $limit = 50,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::georadius($this->resolveConn($conn), $table, $geomColumn, $lon, $lat, $radiusMeters, $limit));
    }

    public function geoadd(
        string $table,
        string $nameColumn,
        string $geomColumn,
        string $name,
        float $lon,
        float $lat,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::geoadd($this->resolveConn($conn), $table, $nameColumn, $geomColumn, $name, $lon, $lat));
    }

    public function geodist(
        string $table,
        string $geomColumn,
        string $nameColumn,
        string $nameA,
        string $nameB,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::geodist($this->resolveConn($conn), $table, $geomColumn, $nameColumn, $nameA, $nameB));
    }

    // Misc

    public function countDistinct(string $table, string $column, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::countDistinct($this->resolveConn($conn), $table, $column));
    }

    /**
     * Run a Lua script via pllua. Like the sync wrapper, the variadic
     * `...$args` argument swallows the trailing `$conn` override — use
     * `$gl->using($conn, fn($gl) => $gl->script(...))` to run on a
     * specific connection.
     */
    public function script(string $luaCode, mixed ...$args): Future
    {
        return async(fn() => Utils::script($this->resolveConn(null), $luaCode, ...$args));
    }

    // Streams

    public function streamAdd(string $stream, array $payload, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::streamAdd($this->resolveConn($conn), $stream, $payload));
    }

    public function streamCreateGroup(string $stream, string $group, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::streamCreateGroup($this->resolveConn($conn), $stream, $group));
    }

    public function streamRead(
        string $stream,
        string $group,
        string $consumer,
        int $count = 1,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(function () use ($stream, $group, $consumer, $count, $conn) {
            $c = $this->resolveConn($conn);
            if (!$c instanceof PostgresConnection) {
                throw new \InvalidArgumentException(
                    'streamRead requires a PostgresConnection (opens its own transaction).'
                );
            }
            return Utils::streamRead($c, $stream, $group, $consumer, $count);
        });
    }

    public function streamAck(string $stream, string $group, int $messageId, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::streamAck($this->resolveConn($conn), $stream, $group, $messageId));
    }

    public function streamClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::streamClaim($this->resolveConn($conn), $stream, $group, $consumer, $minIdleMs));
    }

    // Percolate

    public function percolateAdd(
        string $name,
        string $queryId,
        string $query,
        string $lang = 'english',
        ?array $metadata = null,
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::percolateAdd($this->resolveConn($conn), $name, $queryId, $query, $lang, $metadata));
    }

    public function percolate(
        string $name,
        string $text,
        int $limit = 50,
        string $lang = 'english',
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::percolate($this->resolveConn($conn), $name, $text, $limit, $lang));
    }

    public function percolateDelete(string $name, string $queryId, ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::percolateDelete($this->resolveConn($conn), $name, $queryId));
    }

    // Debug

    public function analyze(string $text, string $lang = 'english', ?PostgresExecutor $conn = null): Future
    {
        return async(fn() => Utils::analyze($this->resolveConn($conn), $text, $lang));
    }

    public function explainScore(
        string $table,
        string $column,
        string $query,
        string $idColumn,
        mixed $idValue,
        string $lang = 'english',
        ?PostgresExecutor $conn = null,
    ): Future {
        return async(fn() => Utils::explainScore($this->resolveConn($conn), $table, $column, $query, $idColumn, $idValue, $lang));
    }
}
