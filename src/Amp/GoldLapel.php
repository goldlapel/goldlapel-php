<?php

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\GoldLapel as SyncGoldLapel;
use Revolt\EventLoop\FiberLocal;
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
 * fiber-local (via Revolt\EventLoop\FiberLocal) — sibling fibers running
 * concurrently on the same GoldLapel instance each see their own scope
 * (or the default connection, outside any `using()`). The scoped conn is
 * resolved in the caller's fiber *before* each method's inner async()
 * fiber is spawned, so the scope propagates naturally via closure capture
 * without leaking to concurrent siblings.
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
    /**
     * Fiber-local scope set by using(); takes precedence over $connection.
     * Each fiber stores its own value, so sibling fibers on the same
     * instance never observe each other's scoped connections.
     *
     * @var FiberLocal<PostgresExecutor|null>
     */
    private FiberLocal $scopedConn;

    /** @var array<int, self> */
    private static array $liveInstances = [];
    private static bool $cleanupRegistered = false;

    /**
     * Stream the startup banner is written to. Defaults to STDERR; tests
     * can swap this for a php://memory stream (or a userland stream
     * wrapper) to capture output, or to inject a throwing write to
     * verify banner-failure cleanup. Mirrors the sync factory.
     *
     * @var resource|null
     */
    private static $bannerStream = null;

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
        $this->scopedConn = new FiberLocal(static fn () => null);
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
                // Mirror the sync factory's inner-guarded cleanup: if
                // terminate() itself throws (a proc_close() edge case or
                // SIGTERM race), we must not mask the original $e with a
                // teardown error. The user-facing failure is the connect
                // failure, not the cleanup failure.
                try {
                    $instance->terminate();
                } catch (\Throwable $cleanupErr) {
                    // Don't mask the original failure with a teardown error.
                }
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
        //
        // `new static(...)` (not `new self(...)`) so test subclasses can
        // override instance methods like terminate() to simulate rare
        // cleanup-throws scenarios from the catch block in start().
        [$port, $config, $extraArgs, $silent] = self::parseStartOptions($options);
        $instance = new static($upstream, $port, $config, $extraArgs, $silent);
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
            throw new RuntimeException(
                "Failed to start Gold Lapel process (proc_open returned false for {$binary}). " .
                "Check that the binary is executable and the system has free resources."
            );
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
                // Print the banner BEFORE registering for cleanup: an fwrite()
                // failure on stderr must not leak an orphan entry in
                // $liveInstances. Registration is the last side-effect so
                // that any throw between "subprocess confirmed listening"
                // and "return $this->url" bubbles out with no registry
                // cleanup required. Mirrors the sync factory.
                $this->printBanner();
                self::registerForCleanup($this);
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
        $stream = self::$bannerStream ?? (defined('STDERR') ? STDERR : null);
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
            throw new RuntimeException(
                'Gold Lapel proxy is not running. Call start() before connect().'
            );
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
            // $this->scopedConn is a FiberLocal — storage is a
            // WeakMap<Fiber, ...> keyed by the owning fiber, so entries
            // are garbage-collected when the fiber itself terminates.
            // (Revolt's driver also calls FiberLocal::clear() after each
            // callback/microtask, but only on the *current* fiber, not
            // "each fiber's on teardown".) Either way, stop() runs in
            // its own async fiber — no other fiber's scope is visible
            // here, and there's nothing to reset.
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

    /**
     * Terminate the subprocess synchronously (SIGTERM → wait → SIGKILL).
     * `protected` (rather than `private`) so test subclasses can override
     * to verify the inner-guarded cleanup in start()'s catch block — if
     * terminate() itself throws, the original connect exception must
     * propagate unchanged.
     */
    protected function terminate(): void
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

    /**
     * Resolve the executor for a method call. Reads the fiber-local
     * `$scopedConn` — MUST be called from the fiber whose scope should
     * apply (i.e. outside any inner `async()` that would start a new
     * fiber with its own empty FiberLocal slot).
     */
    private function resolveConn(?PostgresExecutor $conn): PostgresExecutor
    {
        if ($conn !== null) {
            return $conn;
        }
        $scoped = $this->scopedConn->get();
        if ($scoped !== null) {
            return $scoped;
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
            // Do NOT interpolate the URL — it may contain the upstream password.
            throw new RuntimeException('Cannot parse proxy URL (missing host).');
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
     * The callback runs inside the fiber that `using()`'s own `async()`
     * block creates (not the caller's fiber). `$gl->*` calls made
     * directly from the callback read `FiberLocal` from that same
     * fiber, so they see `$connection` as their default executor. If
     * the callback returns a Future, it is awaited before the scope is
     * restored, so callers can safely return `->await()`ed values.
     *
     * Nested `using()`: an inner `using($Y, ...)` inside an outer
     * `using($X, ...)` runs its callback on a fresh fiber whose
     * FiberLocal slot is set to `$Y` alone — it does NOT see a stacked
     * `$X`+`$Y` view. When the inner call returns, the outer fiber's
     * FiberLocal is untouched, so the outer callback continues to see
     * `$X` as expected.
     */
    public function using(PostgresExecutor $connection, callable $callback): Future
    {
        return async(function () use ($connection, $callback) {
            // Fiber-local set/restore — concurrent fibers each see their
            // own scope. The FiberLocal WeakMap drops this fiber's entry
            // when the fiber is garbage-collected, so the `finally`
            // restore is really only for nested using() on the same
            // fiber.
            $previous = $this->scopedConn->get();
            $this->scopedConn->set($connection);
            try {
                $result = $callback($this);
                if ($result instanceof Future) {
                    $result = $result->await();
                }
                return $result;
            } finally {
                $this->scopedConn->set($previous);
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docInsert($c, $collection, $document));
    }

    public function docInsertMany(string $collection, array $documents, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docInsertMany($c, $collection, $documents));
    }

    public function docFind(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docFind($c, $collection, $filter, $sort, $limit, $skip));
    }

    public function docFindOne(string $collection, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docFindOne($c, $collection, $filter));
    }

    public function docUpdate(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docUpdate($c, $collection, $filter, $update));
    }

    public function docUpdateOne(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docUpdateOne($c, $collection, $filter, $update));
    }

    public function docDelete(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docDelete($c, $collection, $filter));
    }

    public function docDeleteOne(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docDeleteOne($c, $collection, $filter));
    }

    public function docCount(string $collection, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docCount($c, $collection, $filter));
    }

    public function docCreateIndex(string $collection, ?array $keys = null, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docCreateIndex($c, $collection, $keys));
    }

    public function docAggregate(string $collection, array $pipeline, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docAggregate($c, $collection, $pipeline));
    }

    /**
     * Install the change-stream trigger and (optionally) run a long-lived
     * listener. If $callback is non-null, the Future blocks until the
     * listener stops; this requires a full PostgresConnection as the
     * executor.
     */
    public function docWatch(string $collection, ?callable $callback = null, ?PostgresExecutor $conn = null): Future
    {
        // Resolve in the caller's fiber so the fiber-local `using()` scope
        // is honoured (inner async() spawns a fresh fiber with its own
        // empty FiberLocal slot).
        $c = $this->resolveConn($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'docWatch requires a PostgresConnection (LISTEN is not supported on transactions).'
            );
        }
        return async(fn() => Utils::docWatch($c, $collection, $callback));
    }

    public function docUnwatch(string $collection, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docUnwatch($c, $collection));
    }

    public function docCreateTtlIndex(
        string $collection,
        int $expireAfterSeconds,
        string $field = 'created_at',
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docCreateTtlIndex($c, $collection, $expireAfterSeconds, $field));
    }

    public function docRemoveTtlIndex(string $collection, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docRemoveTtlIndex($c, $collection));
    }

    public function docCreateCollection(string $collection, bool $unlogged = false, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docCreateCollection($c, $collection, $unlogged));
    }

    public function docCreateCapped(string $collection, int $maxDocuments, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docCreateCapped($c, $collection, $maxDocuments));
    }

    public function docRemoveCap(string $collection, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docRemoveCap($c, $collection));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docFindOneAndUpdate($c, $collection, $filter, $update));
    }

    public function docFindOneAndDelete(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docFindOneAndDelete($c, $collection, $filter));
    }

    public function docDistinct(string $collection, string $field, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::docDistinct($c, $collection, $field, $filter));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::search($c, $table, $column, $query, $limit, $lang, $highlight));
    }

    public function searchFuzzy(
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        float $threshold = 0.3,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::searchFuzzy($c, $table, $column, $query, $limit, $threshold));
    }

    public function searchPhonetic(
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::searchPhonetic($c, $table, $column, $query, $limit));
    }

    public function similar(
        string $table,
        string $column,
        array $vector,
        int $limit = 10,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::similar($c, $table, $column, $vector, $limit));
    }

    public function suggest(
        string $table,
        string $column,
        string $prefix,
        int $limit = 10,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::suggest($c, $table, $column, $prefix, $limit));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::facets($c, $table, $column, $limit, $query, $queryColumn, $lang));
    }

    public function aggregate(
        string $table,
        string $column,
        string $func,
        ?string $groupBy = null,
        int $limit = 50,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::aggregate($c, $table, $column, $func, $groupBy, $limit));
    }

    public function createSearchConfig(string $name, string $copyFrom = 'english', ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::createSearchConfig($c, $name, $copyFrom));
    }

    // Pub/Sub & Queue

    public function publish(string $channel, string $message, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::publish($c, $channel, $message));
    }

    public function subscribe(string $channel, callable $callback, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'subscribe requires a PostgresConnection (LISTEN is not supported on transactions).'
            );
        }
        return async(fn() => Utils::subscribe($c, $channel, $callback));
    }

    public function enqueue(string $queueTable, array $payload, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::enqueue($c, $queueTable, $payload));
    }

    public function dequeue(string $queueTable, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::dequeue($c, $queueTable));
    }

    // Counters

    public function incr(string $table, string $key, int $amount = 1, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::incr($c, $table, $key, $amount));
    }

    public function getCounter(string $table, string $key, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::getCounter($c, $table, $key));
    }

    // Hash

    public function hset(string $table, string $key, string $field, mixed $value, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::hset($c, $table, $key, $field, $value));
    }

    public function hget(string $table, string $key, string $field, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::hget($c, $table, $key, $field));
    }

    public function hgetall(string $table, string $key, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::hgetall($c, $table, $key));
    }

    public function hdel(string $table, string $key, string $field, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::hdel($c, $table, $key, $field));
    }

    // Sorted Sets

    public function zadd(string $table, string $member, float $score, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::zadd($c, $table, $member, $score));
    }

    public function zincrby(string $table, string $member, float $amount = 1, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::zincrby($c, $table, $member, $amount));
    }

    public function zrange(string $table, int $start = 0, int $stop = 10, bool $desc = true, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::zrange($c, $table, $start, $stop, $desc));
    }

    public function zrank(string $table, string $member, bool $desc = true, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::zrank($c, $table, $member, $desc));
    }

    public function zscore(string $table, string $member, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::zscore($c, $table, $member));
    }

    public function zrem(string $table, string $member, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::zrem($c, $table, $member));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::georadius($c, $table, $geomColumn, $lon, $lat, $radiusMeters, $limit));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::geoadd($c, $table, $nameColumn, $geomColumn, $name, $lon, $lat));
    }

    public function geodist(
        string $table,
        string $geomColumn,
        string $nameColumn,
        string $nameA,
        string $nameB,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::geodist($c, $table, $geomColumn, $nameColumn, $nameA, $nameB));
    }

    // Misc

    public function countDistinct(string $table, string $column, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::countDistinct($c, $table, $column));
    }

    /**
     * Run a Lua script via pllua. Like the sync wrapper, the variadic
     * `...$args` argument swallows the trailing `$conn` override — use
     * `$gl->using($conn, fn($gl) => $gl->script(...))` to run on a
     * specific connection.
     */
    public function script(string $luaCode, mixed ...$args): Future
    {
        $c = $this->resolveConn(null);
        return async(fn() => Utils::script($c, $luaCode, ...$args));
    }

    // Streams

    public function streamAdd(string $stream, array $payload, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::streamAdd($c, $stream, $payload));
    }

    public function streamCreateGroup(string $stream, string $group, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::streamCreateGroup($c, $stream, $group));
    }

    public function streamRead(
        string $stream,
        string $group,
        string $consumer,
        int $count = 1,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'streamRead requires a PostgresConnection (opens its own transaction).'
            );
        }
        return async(fn() => Utils::streamRead($c, $stream, $group, $consumer, $count));
    }

    public function streamAck(string $stream, string $group, int $messageId, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::streamAck($c, $stream, $group, $messageId));
    }

    public function streamClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::streamClaim($c, $stream, $group, $consumer, $minIdleMs));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::percolateAdd($c, $name, $queryId, $query, $lang, $metadata));
    }

    public function percolate(
        string $name,
        string $text,
        int $limit = 50,
        string $lang = 'english',
        ?PostgresExecutor $conn = null,
    ): Future {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::percolate($c, $name, $text, $limit, $lang));
    }

    public function percolateDelete(string $name, string $queryId, ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::percolateDelete($c, $name, $queryId));
    }

    // Debug

    public function analyze(string $text, string $lang = 'english', ?PostgresExecutor $conn = null): Future
    {
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::analyze($c, $text, $lang));
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
        $c = $this->resolveConn($conn);
        return async(fn() => Utils::explainScore($c, $table, $column, $query, $idColumn, $idValue, $lang));
    }
}
