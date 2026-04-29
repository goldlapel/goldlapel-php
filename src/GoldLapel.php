<?php

namespace GoldLapel;

use RuntimeException;

/**
 * Gold Lapel PHP wrapper — factory API.
 *
 * Usage:
 *     $gl = GoldLapel::start('postgresql://user:pass@db/mydb', [
 *         'proxy_port' => 7932,
 *         'log_level' => 'info',
 *     ]);
 *     // PDO needs the pgsql: DSN form plus user/password — not the raw URL.
 *     $pdo = new PDO($gl->pdoDsn(), ...$gl->pdoCredentials());
 *     $hits = $gl->search('articles', 'body', 'postgres tuning');
 *     $gl->documents->insert('events', ['type' => 'signup']);
 *     $gl->streams->add('clicks', ['url' => '/']);
 *     $gl->using($pdo, function ($gl) {
 *         $gl->documents->insert('events', ['type' => 'order.created']);
 *     });
 *     $gl->stop();
 */
class GoldLapel
{
    const DEFAULT_PROXY_PORT = 7932;
    const STARTUP_TIMEOUT = 10.0;
    const STARTUP_POLL_INTERVAL = 0.05;

    // Keys that are valid inside the structured `config` map. Top-level
    // concepts (proxy_port, dashboard_port, invalidation_port, log_level,
    // mode, license, client, config_file) are accepted as top-level options
    // on GoldLapel::start() and are NOT valid keys here — passing them
    // through `config` raises.
    private const VALID_CONFIG_KEYS = [
        'min_pattern_count', 'refresh_interval_secs', 'pattern_ttl_secs',
        'max_tables_per_view', 'max_columns_per_view', 'deep_pagination_threshold',
        'report_interval_secs', 'result_cache_size', 'batch_cache_size',
        'batch_cache_ttl_secs', 'pool_size', 'pool_timeout_secs',
        'pool_mode', 'mgmt_idle_timeout', 'fallback', 'read_after_write_secs',
        'n1_threshold', 'n1_window_ms', 'n1_cross_threshold',
        'tls_cert', 'tls_key', 'tls_client_ca',
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

    /**
     * Stream the startup banner is written to. Defaults to STDERR; tests
     * can swap this for a php://memory stream to capture output without
     * depending on the real stderr file descriptor.
     *
     * @var resource
     */
    private static $bannerStream;

    private string $upstream;
    private int $proxyPort;
    private int $dashboardPort;
    private bool $dashboardPortExplicit;
    private int $invalidationPort;
    private bool $invalidationPortExplicit;
    private ?string $logLevel;
    private ?string $mode;
    private ?string $license;
    private ?string $client;
    private ?string $configFile;
    private bool $silent;
    private bool $mesh;
    private ?string $meshTag;
    private array $config;
    private array $extraArgs;
    /** @var resource|null */
    private $process = null;
    private ?string $url = null;
    private ?\PDO $pdo = null;

    /** Connection set by using(); takes precedence over $pdo. */
    private ?\PDO $scopedConn = null;

    /** Dashboard token, provisioned on startProxy(). Used by Ddl.php. */
    private ?string $dashboardToken = null;

    /** Per-instance cache of fetched DDL patterns keyed on "family:name". */
    private array $ddlCache = [];

    /** @var array<int, self> */
    private static array $liveInstances = [];
    private static bool $cleanupRegistered = false;

    /**
     * Documents sub-API — `$gl->documents-><verb>(...)`. Holds a back-reference
     * to this client; reads dashboard token / port / cache through it. Created
     * once in __construct() and lives for the life of this instance.
     *
     * Schema-to-core Phase 4: doc-store DDL is owned by the proxy. The
     * sub-API issues `/api/ddl/doc_store/create` (idempotent) on first use of
     * each collection and caches the resulting (tables, query_patterns) on
     * this client.
     */
    public readonly Documents $documents;

    /**
     * Streams sub-API — `$gl->streams-><verb>(...)`. Holds a back-reference
     * to this client; reads dashboard token / port / cache through it.
     *
     * Schema-to-core Phase 1+2 shipped streams DDL ownership; Phase 4 moves
     * the surface from flat `streamAdd` etc. into the nested namespace.
     */
    public readonly Streams $streams;

    /**
     * Constructor — use GoldLapel::start() to create and start an instance.
     * Accepts a canonical-surface options array (same shape as start()).
     *
     * @param array{
     *   proxy_port?: int,
     *   dashboard_port?: int,
     *   invalidation_port?: int,
     *   log_level?: string,
     *   mode?: string,
     *   license?: string,
     *   client?: string,
     *   config_file?: string,
     *   config?: array<string, mixed>,
     *   extra_args?: list<string>,
     *   silent?: bool,
     *   mesh?: bool,
     *   mesh_tag?: string,
     * } $options
     */
    public function __construct(string $upstream, array $options = [])
    {
        $this->upstream = $upstream;
        $this->proxyPort = isset($options['proxy_port']) ? (int) $options['proxy_port'] : self::DEFAULT_PROXY_PORT;

        // Dashboard / invalidation ports default to proxyPort + 1 / + 2 when
        // unset. An explicit value (including 0 for "disable dashboard")
        // overrides the derivation and is emitted as --dashboard-port /
        // --invalidation-port at spawn time.
        $this->dashboardPortExplicit = array_key_exists('dashboard_port', $options);
        $this->dashboardPort = $this->dashboardPortExplicit
            ? (int) $options['dashboard_port']
            : $this->proxyPort + 1;
        $this->invalidationPortExplicit = array_key_exists('invalidation_port', $options);
        $this->invalidationPort = $this->invalidationPortExplicit
            ? (int) $options['invalidation_port']
            : $this->proxyPort + 2;

        $this->logLevel = isset($options['log_level']) ? (string) $options['log_level'] : null;
        $this->mode = isset($options['mode']) ? (string) $options['mode'] : null;
        $this->license = isset($options['license']) ? (string) $options['license'] : null;
        $this->client = isset($options['client']) ? (string) $options['client'] : null;
        $this->configFile = isset($options['config_file']) ? (string) $options['config_file'] : null;

        $config = $options['config'] ?? [];
        // Validate structured-config keys eagerly so a test that constructs
        // without spawning still catches bad keys.
        $validKeys = array_flip(self::VALID_CONFIG_KEYS);
        foreach ($config as $key => $_) {
            if (!isset($validKeys[$key])) {
                throw new \InvalidArgumentException("Unknown config key: {$key}");
            }
        }
        $this->config = $config;
        $this->extraArgs = $options['extra_args'] ?? [];
        $this->silent = !empty($options['silent']);
        // Mesh membership (startup intent — HQ enforces license).
        $this->mesh = !empty($options['mesh']);
        $tag = isset($options['mesh_tag']) ? (string) $options['mesh_tag'] : '';
        $this->meshTag = $tag === '' ? null : $tag;

        // Nested namespaces — see src/Documents.php and src/Streams.php.
        // These are the canonical schema-to-core sub-API instances. Each
        // holds a back-reference to this client for shared state (license,
        // dashboard token, PDO, DDL pattern cache). Other namespaces (cache,
        // search, queues, counters, hashes, zsets, geo, auth, ...) stay flat
        // for now; they migrate to nested form one-at-a-time as their own
        // schema-to-core phase fires.
        $this->documents = new Documents($this);
        $this->streams = new Streams($this);
    }

    /**
     * Factory — start a Gold Lapel proxy and return a ready-to-use instance.
     *
     * Top-level options (all optional):
     *   - 'proxy_port' (int): proxy port (default 7932)
     *   - 'dashboard_port' (int): dashboard port. Defaults to proxy_port + 1. 0 disables.
     *   - 'invalidation_port' (int): cache-invalidation port. Defaults to proxy_port + 2.
     *   - 'log_level' (string): 'trace'|'debug'|'info'|'warn'|'error' — translated to the proxy's -v/-vv/-vvv verbosity flag
     *   - 'mode' (string): proxy operating mode ('waiter', 'consideration')
     *   - 'license' (string): path to a signed license file
     *   - 'client' (string): client identifier for telemetry tagging
     *   - 'config_file' (string): path to a TOML config file (passed as --config)
     *   - 'config' (array): structured tuning keys — must be one of configKeys()
     *   - 'extra_args' (array): raw extra CLI flags
     *   - 'silent' (bool): suppress the startup banner
     *   - 'mesh' (bool): opt into the mesh at startup (HQ enforces license; denial is non-fatal)
     *   - 'mesh_tag' (string): optional mesh tag — instances with the same tag cluster together
     *
     * Promoted top-level concepts (proxy_port, dashboard_port, etc.) are NOT
     * valid keys inside `config` — passing them there raises at construction
     * time. This matches the canonical config surface across all wrappers.
     *
     * Eagerly opens a PDO connection to the proxy; raises RuntimeException
     * if pdo_pgsql is not enabled.
     */
    public static function start(string $upstream, array $options = []): self
    {
        $instance = new self($upstream, $options);
        // startProxyWithoutConnect() (invoked via startProxy) registers the
        // instance with cleanupAll as soon as the subprocess spawns, before
        // the PDO is opened. If the subsequent PDO construction fails we
        // tear the subprocess down *immediately* here rather than leaving
        // it for the shutdown hook — matching the sync Python and Ruby
        // wrappers. A long-running PHP process (Octane, Swoole, RoadRunner,
        // a CLI daemon) that repeatedly calls start() against a bad
        // upstream would otherwise accumulate orphan goldlapel children
        // until the PHP process itself exited. The $liveInstances registry
        // keeps a reference to $instance, so PHP's deterministic destructor
        // does NOT fire when start() throws — explicit cleanup here is the
        // only path that runs at throw-time.
        try {
            $instance->startProxy();
        } catch (\Throwable $e) {
            try {
                $instance->stop();
            } catch (\Throwable $cleanupErr) {
                // Don't mask the original failure with a teardown error.
            }
            throw $e;
        }

        return $instance;
    }

    /**
     * Low-level factory variant: start the proxy and return the instance
     * without opening an internal PDO. Useful when you want to manage your
     * own PDO connections, or in environments (like Laravel's service
     * provider) where PDOs are created later by the framework.
     *
     * The instance's URL is available via `$gl->url()`. Callers that hold
     * onto the returned instance can call `$gl->stop()` at worker shutdown
     * (Octane, Swoole, RoadRunner) to release the subprocess deterministically
     * rather than relying on `__destruct` or the process-wide shutdown hook.
     */
    public static function startProxyOnly(string $upstream, array $options = []): self
    {
        $instance = new self($upstream, $options);
        $instance->startProxyWithoutConnect();

        return $instance;
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
     * Public alias of resolveConn() for the sub-API namespace classes
     * (Documents, Streams). PHP doesn't have package-private visibility, so
     * we expose this with a `Public` suffix to discourage user code from
     * calling it — pass an explicit `?\PDO $conn` to verb methods instead,
     * or use `using()` for scoped overrides.
     *
     * @internal Used only by GoldLapel\Documents and GoldLapel\Streams.
     */
    public function resolveConnPublic(?\PDO $conn): \PDO
    {
        return $this->resolveConn($conn);
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
        $cmd = [$binary, '--upstream', $this->upstream, '--proxy-port', (string) $this->proxyPort];

        // Top-level options (promoted out of the config map by the canonical
        // surface) emit their own CLI flags before the tuning-knob config
        // map. Each is suppressed when the user hasn't set it, so the Rust
        // binary applies its own defaults.
        if ($this->dashboardPortExplicit) {
            $cmd[] = '--dashboard-port';
            $cmd[] = (string) $this->dashboardPort;
        }
        if ($this->invalidationPortExplicit) {
            $cmd[] = '--invalidation-port';
            $cmd[] = (string) $this->invalidationPort;
        }
        if ($this->logLevel !== null) {
            $verboseFlag = self::translateLogLevel($this->logLevel);
            if ($verboseFlag !== null) {
                $cmd[] = $verboseFlag;
            }
        }
        if ($this->mode !== null) {
            $cmd[] = '--mode';
            $cmd[] = $this->mode;
        }
        if ($this->license !== null) {
            $cmd[] = '--license';
            $cmd[] = $this->license;
        }
        if ($this->client !== null) {
            $cmd[] = '--client';
            $cmd[] = $this->client;
        }
        if ($this->configFile !== null) {
            $cmd[] = '--config';
            $cmd[] = $this->configFile;
        }
        if ($this->mesh) {
            $cmd[] = '--mesh';
        }
        if ($this->meshTag !== null) {
            $cmd[] = '--mesh-tag';
            $cmd[] = $this->meshTag;
        }
        $cmd = array_merge($cmd, self::configToArgs($this->config), $this->extraArgs);

        $nullDevice = PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null';
        $descriptors = [
            0 => ['file', $nullDevice, 'r'],
            1 => ['file', $nullDevice, 'w'],
            2 => ['pipe', 'w'],
        ];

        $env = getenv();
        // GOLDLAPEL_CLIENT env var is only set when the user hasn't opted
        // in via the top-level `client` option (which emits --client and
        // takes precedence over the env var).
        if ($this->client === null && !isset($env['GOLDLAPEL_CLIENT'])) {
            $env['GOLDLAPEL_CLIENT'] = 'php';
        }
        // Provision a session-scoped dashboard token so Ddl.php can authenticate
        // against /api/ddl/* without a file on disk. Pre-set env wins.
        if (!empty($env['GOLDLAPEL_DASHBOARD_TOKEN'])) {
            $this->dashboardToken = $env['GOLDLAPEL_DASHBOARD_TOKEN'];
        } else {
            $this->dashboardToken = bin2hex(random_bytes(32));
            $env['GOLDLAPEL_DASHBOARD_TOKEN'] = $this->dashboardToken;
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

            if (self::waitForPort('127.0.0.1', $this->proxyPort, 0.5)) {
                fclose($stderr);
                $this->url = self::makeProxyUrl($this->upstream, $this->proxyPort);

                // Print the banner BEFORE registering for cleanup: fwrite()
                // to stderr is vanishingly unlikely to fail, but if it does
                // (closed fd in a long-running SAPI, unwritable stream, FPM
                // after fastcgi_finish_request() has detached stderr) we'd
                // rather let the throw escape with no registry entry than
                // leak an orphan reference into $liveInstances. Registration
                // is the last side-effect because it's the one whose cleanup
                // is most expensive to get wrong.
                self::printBanner($this->proxyPort, $this->dashboardPort, $this->silent);

                // Register the instance for global cleanup immediately so
                // that any subsequent init step (e.g. PDO construction in
                // startProxy()) can throw without leaking the subprocess.
                self::registerForCleanup($this);

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
            "Gold Lapel failed to start on port {$this->proxyPort} within " . self::STARTUP_TIMEOUT . "s.\nstderr: {$stderrOutput}"
        );
    }

    /**
     * Emit the startup banner to stderr. In a PHP-FPM / web SAPI context
     * stdout becomes the HTTP response body, so we route this to stderr to
     * avoid corrupting JSON output, injecting whitespace before a <!doctype>,
     * or triggering "headers already sent" errors. Respects the `silent`
     * wrapper option — default false (banner prints to stderr).
     */
    private static function printBanner(int $port, int $dashboardPort, bool $silent): void
    {
        if ($silent) {
            return;
        }

        $message = $dashboardPort > 0
            ? "goldlapel → :{$port} (proxy) | http://127.0.0.1:{$dashboardPort} (dashboard)\n"
            : "goldlapel → :{$port} (proxy)\n";

        $stream = self::$bannerStream ?? (defined('STDERR') ? STDERR : null);
        if (is_resource($stream)) {
            fwrite($stream, $message);
        }
    }

    /**
     * Stop the proxy for this instance. Idempotent.
     */
    public function stop(): void
    {
        $this->pdo = null;
        $this->scopedConn = null;
        // Drop cached DDL patterns — they're tied to the proxy we're about
        // to terminate.
        $this->ddlCache = [];
        $this->dashboardToken = null;

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

    public function dashboardToken(): ?string
    {
        return $this->dashboardToken;
    }

    /** @return array<string, array<string, mixed>> by-reference */
    public function &ddlCache(): array
    {
        return $this->ddlCache;
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
     * Proxy URL in `postgresql://user:pass@host:port/db` form — hand this to
     * any Postgres driver (pgx, libpq, Doctrine DBAL, etc.). PHP's PDO does
     * *not* accept this format directly: for `new PDO(...)` use
     * `$gl->pdoDsn()` plus `$gl->pdoCredentials()`.
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

    public function getProxyPort(): int
    {
        return $this->proxyPort;
    }

    public function getDashboardPort(): int
    {
        return $this->dashboardPort;
    }

    public function getInvalidationPort(): int
    {
        return $this->invalidationPort;
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
        if (!$status['running']) {
            return false;
        }

        // Zombie detection. On some systems `proc_get_status()` returns
        // `running => true` for a child that has already exited but hasn't
        // been `wait()`ed on yet. Two cross-checks catch that:
        //
        //   1. If `exitcode` is anything other than -1, the process has been
        //      reaped by a prior `proc_get_status()` call and the cached
        //      exit status is being replayed — it's definitively not
        //      running anymore. PHP contractually reports exitcode = -1
        //      while the child is alive.
        //
        //   2. On POSIX hosts, `posix_kill($pid, 0)` returns false when the
        //      kernel has no such pid at all (reaped zombie or never
        //      existed). A live-or-zombie process returns true. Combined
        //      with check #1, a false here means the process is truly gone.
        //
        // Either check alone can have false negatives (check #1 during the
        // race window before exitcode is cached; check #2 for a zombie that
        // hasn't been reaped yet), so we treat a positive signal from
        // either as "not running".
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
                : $this->proxyPort + 2;
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

    // Document Store: $gl->documents-><verb>(...). See src/Documents.php.

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

    /**
     * Run a Lua script via `pllua`. Positional `$args` are forwarded as the
     * script's text parameters.
     *
     * **No `conn:` override.** Every other wrapper method accepts an optional
     * trailing `?\PDO $conn = null`, but `script()`'s variadic `...$args`
     * would swallow any trailing `\PDO` instead of treating it as the
     * override, so the signature intentionally omits it. The script always
     * runs on the resolved ambient connection — either the `using()` scope
     * (if set) or the factory-managed internal PDO.
     *
     * To run `script()` on a specific connection, wrap the call in `using()`:
     *
     *     $gl->using($myPdo, fn ($gl) => $gl->script($lua, 'arg1', 'arg2'));
     *
     * Or call the underlying static helper directly for one-off use:
     *
     *     Utils::script($myPdo, $lua, 'arg1', 'arg2');
     */
    public function script(string $luaCode, mixed ...$args): ?string
    {
        return Utils::script($this->resolveConn(null), $luaCode, ...$args);
    }

    // Streams: $gl->streams-><verb>(...). See src/Streams.php.

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
