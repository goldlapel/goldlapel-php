<?php

namespace GoldLapel;

class NativeCache
{
    const DDL_SENTINEL = '__ddl__';

    private const TX_START = '/^\s*(BEGIN|START\s+TRANSACTION)\b/i';
    private const TX_END = '/^\s*(COMMIT|ROLLBACK|END)\b/i';
    private const TABLE_PATTERN = '/\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)/i';

    private const SQL_KEYWORDS = [
        'select', 'from', 'where', 'and', 'or', 'not', 'in', 'exists',
        'between', 'like', 'is', 'null', 'true', 'false', 'as', 'on',
        'left', 'right', 'inner', 'outer', 'cross', 'full', 'natural',
        'group', 'order', 'having', 'limit', 'offset', 'union', 'intersect',
        'except', 'all', 'distinct', 'lateral', 'values',
    ];

    // --- L1 telemetry tuning ---
    //
    // Demand-driven model (mirrors goldlapel-python). Counters bump on
    // cache ops; state-change events emit synchronously when a relevant
    // counter crosses a threshold; snapshot replies are sent only when
    // the proxy asks via ?:<request>.
    //
    // Eviction-rate sliding window: cache_full fires when ≥ 50% of the
    // last 200 puts caused an eviction; cache_recovered when the rate
    // falls back below 10%. Hysteresis prevents flapping at the boundary.
    private const EVICT_RATE_WINDOW = 200;
    private const EVICT_RATE_HIGH = 0.5;
    private const EVICT_RATE_LOW = 0.1;

    private array $cache = [];
    private array $tableIndex = [];
    private array $accessOrder = [];
    private int $counter = 0;
    private int $maxEntries;
    private bool $enabled;
    private bool $invalidationConnected = false;
    private int $invalidationPort = 0;
    /** @var resource|null */
    private $socket = null;
    private bool $stopped = false;

    public int $statsHits = 0;
    public int $statsMisses = 0;
    public int $statsInvalidations = 0;
    // L1 telemetry — was missing before; bumped in evictOne(). Public so
    // tests + external integrations can read it the same way they read
    // the other counters.
    public int $statsEvictions = 0;

    // L1 telemetry state. wrapperId is generated once per process and
    // stable across reconnects; lets the proxy aggregate per-wrapper.
    private string $wrapperId;
    private string $wrapperLang = 'php';
    private string $wrapperVersion;
    // Whether snapshot replies + S: emissions should actually go on the
    // wire. Defaults to true under CLI (long-running workers, Amp,
    // tests, scripts) and false under short-lived SAPIs (FPM, Apache,
    // CGI) where each request would emit wrapper_connected /
    // wrapper_disconnected and flood the proxy. Override with
    // GOLDLAPEL_REPORT_STATS=true|false.
    private bool $reportStats;
    // Sliding window for eviction-rate state-change detection. A bounded
    // ring buffer (1 = evicted, 0 = inserted); updates are O(1).
    private array $recentEvictions = [];
    private int $recentEvictionsIdx = 0;
    // Latched state — only emit a transition when the rate flips. Without
    // latching we'd re-emit cache_full on every put while the rate is bad.
    private bool $stateCacheFull = false;
    // Whether we've already registered a shutdown hook for this instance.
    // PHP can call register_shutdown_function multiple times safely, but
    // we still gate on this so reset() + new instance don't double-register.
    private bool $shutdownRegistered = false;

    private static ?self $instance = null;

    public function __construct(?int $maxEntries = null, ?bool $enabled = null)
    {
        $this->maxEntries = $maxEntries ?? (int) (getenv('GOLDLAPEL_NATIVE_CACHE_SIZE') ?: '32768');
        $envEnabled = getenv('GOLDLAPEL_NATIVE_CACHE');
        $this->enabled = $enabled ?? ($envEnabled === false || strtolower($envEnabled) !== 'false');
        $this->wrapperId = self::generateUuidV4();
        $this->wrapperVersion = self::resolveWrapperVersion();
        $this->reportStats = self::resolveReportStats();
    }

    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public static function reset(): void
    {
        if (self::$instance !== null) {
            self::$instance->disconnect();
        }
        self::$instance = null;
    }

    public function isConnected(): bool
    {
        return $this->invalidationConnected;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function size(): int
    {
        return count($this->cache);
    }

    // --- Cache operations ---

    public function get(string $sql, ?array $params = null): ?array
    {
        if (!$this->enabled || !$this->invalidationConnected) {
            return null;
        }
        $key = self::makeKey($sql, $params);
        if ($key === null) {
            return null;
        }
        if (isset($this->cache[$key])) {
            $this->counter++;
            $this->accessOrder[$key] = $this->counter;
            $this->statsHits++;
            return $this->cache[$key];
        }
        $this->statsMisses++;
        return null;
    }

    public function put(string $sql, ?array $params, array $rows, ?array $columns): void
    {
        if (!$this->enabled || !$this->invalidationConnected) {
            return;
        }
        $key = self::makeKey($sql, $params);
        if ($key === null) {
            return;
        }
        $tables = self::extractTables($sql);
        $evicted = 0;
        if (!isset($this->cache[$key]) && count($this->cache) >= $this->maxEntries) {
            $this->evictOne();
            $evicted = 1;
        }
        $this->cache[$key] = ['rows' => $rows, 'columns' => $columns, 'tables' => $tables];
        $this->counter++;
        $this->accessOrder[$key] = $this->counter;
        foreach ($tables as $table) {
            if (!isset($this->tableIndex[$table])) {
                $this->tableIndex[$table] = [];
            }
            $this->tableIndex[$table][$key] = true;
        }
        $this->recordEviction($evicted);
        // Threshold check happens after the put completes — emitLine may
        // write to the socket and we don't want to interleave write
        // bookkeeping with cache state.
        $this->maybeEmitEvictionRateStateChange();
    }

    public function invalidateTable(string $table): void
    {
        $table = strtolower($table);
        $keys = $this->tableIndex[$table] ?? null;
        if ($keys === null) {
            return;
        }
        unset($this->tableIndex[$table]);
        foreach (array_keys($keys) as $key) {
            $entry = $this->cache[$key] ?? null;
            unset($this->cache[$key]);
            unset($this->accessOrder[$key]);
            if ($entry !== null) {
                foreach ($entry['tables'] as $otherTable) {
                    if ($otherTable !== $table && isset($this->tableIndex[$otherTable])) {
                        unset($this->tableIndex[$otherTable][$key]);
                        if (empty($this->tableIndex[$otherTable])) {
                            unset($this->tableIndex[$otherTable]);
                        }
                    }
                }
            }
        }
        $this->statsInvalidations += count($keys);
    }

    public function invalidateAll(): void
    {
        $count = count($this->cache);
        $this->cache = [];
        $this->tableIndex = [];
        $this->accessOrder = [];
        $this->statsInvalidations += $count;
    }

    // --- Invalidation connection ---

    public function connectInvalidation(int $port): void
    {
        $this->invalidationPort = $port;
        $this->stopped = false;
        $sockPath = "/tmp/goldlapel-{$port}.sock";

        $sock = null;
        try {
            if (PHP_OS_FAMILY !== 'Windows' && file_exists($sockPath)) {
                $sock = @stream_socket_client("unix://{$sockPath}", $errno, $errstr, 1);
            } else {
                $sock = @stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $errstr, 1);
            }

            if ($sock === false) {
                return;
            }

            stream_set_blocking($sock, false);
            $this->socket = $sock;
            $this->invalidationConnected = true;
            $this->registerShutdownEmit();
            $this->emitStateChange('wrapper_connected');

            // Non-blocking read of any pending signals
            $this->drainSignals($sock);
        } catch (\Throwable $e) {
            $this->invalidationConnected = false;
            if ($sock && is_resource($sock)) {
                fclose($sock);
            }
            $this->socket = null;
        }
    }

    public function connectInvalidationPersistent(int $port): void
    {
        $this->invalidationPort = $port;
        $this->invalidationConnected = false;
        $this->stopped = false;
        $sockPath = "/tmp/goldlapel-{$port}.sock";

        $sock = null;
        try {
            if (PHP_OS_FAMILY !== 'Windows' && file_exists($sockPath)) {
                $sock = @stream_socket_client("unix://{$sockPath}", $errno, $errstr, 1);
            } else {
                $sock = @stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $errstr, 1);
            }

            if ($sock === false) {
                return;
            }

            stream_set_blocking($sock, false);
            $this->socket = $sock;
            $this->invalidationConnected = true;
            $this->registerShutdownEmit();
            $this->emitStateChange('wrapper_connected');

            // For long-running processes: read signals using stream_select
            $this->readSignalsLoop($sock);
        } catch (\Throwable $e) {
            if ($this->invalidationConnected) {
                $this->invalidationConnected = false;
                $this->invalidateAll();
            }
        } finally {
            // Clean up — close the current socket (which may have been
            // reconnected, so use $this->socket rather than the original $sock)
            $this->disconnect();
        }
    }

    public function pollSignals(): void
    {
        if ($this->socket !== null && is_resource($this->socket)) {
            $this->drainSignals($this->socket);
        }
    }

    public function disconnect(): void
    {
        $this->stopped = true;
        // Emit wrapper_disconnected before tearing the socket down — the
        // emit path checks invalidationConnected indirectly (via
        // emitLine reading $this->socket), so we sequence:
        //   1. emit while the socket is still up
        //   2. flip the connected flag
        //   3. close the socket
        // emitLine itself short-circuits if reportStats is off, so this
        // is a no-op under FPM.
        if ($this->socket !== null && is_resource($this->socket)) {
            $this->emitWrapperDisconnected();
        }
        $this->invalidationConnected = false;
        if ($this->socket !== null && is_resource($this->socket)) {
            fclose($this->socket);
        }
        $this->socket = null;
    }

    public function setConnected(bool $connected): void
    {
        $this->invalidationConnected = $connected;
    }

    public function processSignal(string $line): void
    {
        // Backwards-compat: unknown prefixes are silently ignored. Older
        // proxies sent only I:/C:/P:; newer proxies may add request types.
        // Forward-compat: accept any well-formed prefix and route by type.
        if (str_starts_with($line, 'I:')) {
            $table = trim(substr($line, 2));
            if ($table === '*') {
                $this->invalidateAll();
            } else {
                $this->invalidateTable($table);
            }
        } elseif (str_starts_with($line, '?:')) {
            $this->processRequest(substr($line, 2));
        }
        // C: (config), P: (ping), and anything else — ignored.
    }

    // --- SQL parsing (static) ---

    public static function makeKey(string $sql, ?array $params = null): ?string
    {
        if ($params === null || empty($params)) {
            return $sql . "\0null";
        }
        return $sql . "\0" . serialize($params);
    }

    public static function detectWrite(string $sql): ?string
    {
        $trimmed = trim($sql);
        $tokens = preg_split('/\s+/', $trimmed);
        if (empty($tokens) || $tokens[0] === '') {
            return null;
        }
        $first = strtoupper($tokens[0]);

        switch ($first) {
            case 'INSERT':
                if (count($tokens) < 3 || strtoupper($tokens[1]) !== 'INTO') {
                    return null;
                }
                return self::bareTable($tokens[2]);
            case 'UPDATE':
                if (count($tokens) < 2) {
                    return null;
                }
                return self::bareTable($tokens[1]);
            case 'DELETE':
                if (count($tokens) < 3 || strtoupper($tokens[1]) !== 'FROM') {
                    return null;
                }
                return self::bareTable($tokens[2]);
            case 'TRUNCATE':
                if (count($tokens) < 2) {
                    return null;
                }
                if (strtoupper($tokens[1]) === 'TABLE') {
                    if (count($tokens) < 3) {
                        return null;
                    }
                    return self::bareTable($tokens[2]);
                }
                return self::bareTable($tokens[1]);
            case 'CREATE':
            case 'ALTER':
            case 'DROP':
            case 'REFRESH':
            case 'DO':
            case 'CALL':
                return self::DDL_SENTINEL;
            case 'MERGE':
                if (count($tokens) < 3 || strtoupper($tokens[1]) !== 'INTO') {
                    return null;
                }
                return self::bareTable($tokens[2]);
            case 'SELECT':
                $sawInto = false;
                $intoTarget = null;
                for ($i = 1; $i < count($tokens); $i++) {
                    $upper = strtoupper($tokens[$i]);
                    if ($upper === 'INTO' && !$sawInto) {
                        $sawInto = true;
                        continue;
                    }
                    if ($sawInto && $intoTarget === null) {
                        if (in_array($upper, ['TEMPORARY', 'TEMP', 'UNLOGGED'], true)) {
                            continue;
                        }
                        $intoTarget = $tokens[$i];
                        continue;
                    }
                    if ($sawInto && $intoTarget !== null && $upper === 'FROM') {
                        return self::DDL_SENTINEL;
                    }
                    if ($upper === 'FROM') {
                        return null;
                    }
                }
                return null;
            case 'COPY':
                if (count($tokens) < 2) {
                    return null;
                }
                $raw = $tokens[1];
                if (str_starts_with($raw, '(')) {
                    return null;
                }
                $tablePart = explode('(', $raw)[0];
                for ($i = 2; $i < count($tokens); $i++) {
                    $upper = strtoupper($tokens[$i]);
                    if ($upper === 'FROM') {
                        return self::bareTable($tablePart);
                    }
                    if ($upper === 'TO') {
                        return null;
                    }
                }
                return null;
            case 'WITH':
                $restUpper = strtoupper(substr($trimmed, strlen($tokens[0])));
                foreach (preg_split('/\s+/', $restUpper) as $token) {
                    $word = ltrim($token, '(');
                    if (in_array($word, ['INSERT', 'UPDATE', 'DELETE'], true)) {
                        return self::DDL_SENTINEL;
                    }
                }
                return null;
            default:
                return null;
        }
    }

    public static function bareTable(string $raw): string
    {
        $table = explode('(', $raw)[0];
        $parts = explode('.', $table);
        $table = end($parts);
        return strtolower($table);
    }

    public static function extractTables(string $sql): array
    {
        $tables = [];
        $keywords = array_flip(self::SQL_KEYWORDS);
        if (preg_match_all(self::TABLE_PATTERN, $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $match) {
                $table = strtolower($match[2]);
                if (!isset($keywords[$table])) {
                    $tables[$table] = true;
                }
            }
        }
        return array_keys($tables);
    }

    public static function isTxStart(string $sql): bool
    {
        return (bool) preg_match(self::TX_START, $sql);
    }

    public static function isTxEnd(string $sql): bool
    {
        return (bool) preg_match(self::TX_END, $sql);
    }

    // --- Private helpers ---

    private function evictOne(): void
    {
        if (empty($this->accessOrder)) {
            return;
        }
        $lruKey = array_keys($this->accessOrder, min($this->accessOrder))[0];
        $entry = $this->cache[$lruKey] ?? null;
        unset($this->cache[$lruKey]);
        unset($this->accessOrder[$lruKey]);
        if ($entry !== null) {
            foreach ($entry['tables'] as $table) {
                if (isset($this->tableIndex[$table])) {
                    unset($this->tableIndex[$table][$lruKey]);
                    if (empty($this->tableIndex[$table])) {
                        unset($this->tableIndex[$table]);
                    }
                }
            }
        }
        $this->statsEvictions++;
    }

    private function drainSignals($sock): void
    {
        $buf = '';
        while (true) {
            $read = [$sock];
            $write = null;
            $except = null;
            $ready = @stream_select($read, $write, $except, 0, 0);
            if ($ready === false || $ready === 0) {
                break;
            }
            $data = @fread($sock, 4096);
            if ($data === false || $data === '') {
                break;
            }
            $buf .= $data;
            while (($pos = strpos($buf, "\n")) !== false) {
                $line = substr($buf, 0, $pos);
                $buf = substr($buf, $pos + 1);
                $this->processSignal($line);
            }
        }
    }

    private function readSignalsLoop($sock): void
    {
        $buf = '';
        while ($this->invalidationConnected && !$this->stopped) {
            $read = [$sock];
            $write = null;
            $except = null;
            $ready = @stream_select($read, $write, $except, 30);
            if ($ready === false) {
                // stream_select error — try to reconnect
                if (!$this->reconnect($sock)) {
                    break;
                }
                $sock = $this->socket;
                $buf = '';
                continue;
            }
            if ($ready === 0) {
                // Timeout — check if socket is still alive with a zero-timeout peek
                if (!is_resource($sock) || feof($sock)) {
                    if (!$this->reconnect($sock)) {
                        break;
                    }
                    $sock = $this->socket;
                    $buf = '';
                }
                continue;
            }
            $data = @fread($sock, 4096);
            if ($data === false || $data === '') {
                // Connection lost — try to reconnect
                if (!$this->reconnect($sock)) {
                    break;
                }
                $sock = $this->socket;
                $buf = '';
                continue;
            }
            $buf .= $data;
            while (($pos = strpos($buf, "\n")) !== false) {
                $line = substr($buf, 0, $pos);
                $buf = substr($buf, $pos + 1);
                $this->processSignal($line);
            }
        }
    }

    private function reconnect($oldSock): bool
    {
        if ($this->stopped) {
            return false;
        }

        if ($oldSock && is_resource($oldSock)) {
            @fclose($oldSock);
        }
        $this->socket = null;
        $this->invalidationConnected = false;
        $this->invalidateAll();

        $port = $this->invalidationPort;
        $sockPath = "/tmp/goldlapel-{$port}.sock";
        $maxAttempts = 10;

        for ($attempt = 0; $attempt < $maxAttempts && !$this->stopped; $attempt++) {
            usleep(min(100000 * (1 << $attempt), 5000000)); // exponential backoff, max 5s

            $sock = null;
            try {
                if (PHP_OS_FAMILY !== 'Windows' && file_exists($sockPath)) {
                    $sock = @stream_socket_client("unix://{$sockPath}", $errno, $errstr, 1);
                } else {
                    $sock = @stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $errstr, 1);
                }

                if ($sock === false) {
                    continue;
                }

                stream_set_blocking($sock, false);
                $this->socket = $sock;
                $this->invalidationConnected = true;
                $this->emitStateChange('wrapper_connected');
                return true;
            } catch (\Throwable $e) {
                if ($sock && is_resource($sock)) {
                    @fclose($sock);
                }
                continue;
            }
        }

        return false;
    }

    // ------------------------------------------------------------------
    // L1 telemetry
    // ------------------------------------------------------------------

    /**
     * Read-only accessor for the stable wrapper id. UUID v4 generated
     * once per process; lets the proxy aggregate per-wrapper across
     * reconnects.
     */
    public function getWrapperId(): string
    {
        return $this->wrapperId;
    }

    public function isReportingStats(): bool
    {
        return $this->reportStats;
    }

    /**
     * Build the L1 snapshot dict the proxy aggregates per-tick. All
     * counters + cache size read in a single pass for internal
     * consistency. Public so external dashboards can call it directly.
     */
    public function buildSnapshot(): array
    {
        return [
            'wrapper_id' => $this->wrapperId,
            'lang' => $this->wrapperLang,
            'version' => $this->wrapperVersion,
            'hits' => $this->statsHits,
            'misses' => $this->statsMisses,
            'evictions' => $this->statsEvictions,
            'invalidations' => $this->statsInvalidations,
            'current_size_entries' => count($this->cache),
            'capacity_entries' => $this->maxEntries,
        ];
    }

    /**
     * Emit a final wrapper_disconnected snapshot before shutdown. Best
     * effort — the socket may already be torn down. Called from
     * disconnect() and from the registered shutdown hook.
     */
    public function emitWrapperDisconnected(): void
    {
        $this->emitStateChange('wrapper_disconnected');
    }

    /**
     * Process a ?:<request> from the proxy. Today the only request is
     * `snapshot` (the proxy asks for current counters; we reply with
     * R:<json>). Future request types can extend this without breaking
     * older proxies — they just won't be expecting the reply.
     */
    private function processRequest(string $body): void
    {
        $body = trim($body);
        if ($body === '' || $body === 'snapshot') {
            $this->emitResponse();
        }
    }

    /**
     * Emit S:<json> with snapshot + state name. Subclasses may override
     * emitLine() to capture the line for testing.
     */
    private function emitStateChange(string $state): void
    {
        if (!$this->reportStats) {
            return;
        }
        $payload = $this->buildSnapshot();
        $payload['state'] = $state;
        $payload['ts_ms'] = (int) (microtime(true) * 1000);
        $encoded = self::encodeJson($payload);
        if ($encoded === null) {
            return;
        }
        $this->emitLine('S:' . $encoded);
    }

    /**
     * Emit R:<json> snapshot reply to a ?:<request>.
     */
    private function emitResponse(): void
    {
        if (!$this->reportStats) {
            return;
        }
        $payload = $this->buildSnapshot();
        $payload['ts_ms'] = (int) (microtime(true) * 1000);
        $encoded = self::encodeJson($payload);
        if ($encoded === null) {
            return;
        }
        $this->emitLine('R:' . $encoded);
    }

    /**
     * Best-effort write of a single newline-delimited line to the
     * invalidation socket. Socket errors are swallowed (the recv path
     * will detect the dead connection and the reconnect logic will
     * rebuild). Visibility is `protected` so test subclasses can capture
     * emissions without needing a real socket.
     */
    protected function emitLine(string $line): void
    {
        if (!$this->reportStats) {
            return;
        }
        $sock = $this->socket;
        if ($sock === null || !is_resource($sock)) {
            return;
        }
        $data = str_ends_with($line, "\n") ? $line : ($line . "\n");
        // PHP without pthreads is single-threaded — no send-lock needed
        // outside the Amp event loop. Within a single fiber, fwrite is
        // atomic for stream resources. Two fibers writing concurrently
        // on the same stream would race, but the cache is only ever
        // written to from the calling fiber (Amp's CachedConnection
        // serializes its calls), so this is safe.
        @fwrite($sock, $data);
    }

    /**
     * Record a put() outcome (1 = evicted, 0 = inserted) into the
     * bounded sliding-window ring. Append until full, then overwrite
     * oldest in O(1).
     */
    private function recordEviction(int $evicted): void
    {
        if (count($this->recentEvictions) < self::EVICT_RATE_WINDOW) {
            $this->recentEvictions[] = $evicted;
        } else {
            $this->recentEvictions[$this->recentEvictionsIdx] = $evicted;
            $this->recentEvictionsIdx = ($this->recentEvictionsIdx + 1) % self::EVICT_RATE_WINDOW;
        }
    }

    /**
     * Check the eviction-rate sliding window and emit a state change if
     * the latched flag should flip. Hysteresis-guarded: crossing HIGH
     * emits cache_full, falling back below LOW emits cache_recovered;
     * rates between LOW and HIGH leave the latched state unchanged.
     */
    private function maybeEmitEvictionRateStateChange(): void
    {
        $n = count($this->recentEvictions);
        if ($n < self::EVICT_RATE_WINDOW) {
            // Warmup gate — a single eviction in 3 puts is noise.
            return;
        }
        $rate = array_sum($this->recentEvictions) / $n;
        $emit = null;
        if (!$this->stateCacheFull && $rate >= self::EVICT_RATE_HIGH) {
            $this->stateCacheFull = true;
            $emit = 'cache_full';
        } elseif ($this->stateCacheFull && $rate <= self::EVICT_RATE_LOW) {
            $this->stateCacheFull = false;
            $emit = 'cache_recovered';
        }
        if ($emit !== null) {
            $this->emitStateChange($emit);
        }
    }

    /**
     * Register the shutdown hook that emits wrapper_disconnected on
     * graceful process exit. Idempotent — called on every connect, but
     * the actual register_shutdown_function only fires once per
     * instance. The hook captures `$this` weakly via WeakReference so a
     * reset() that forgets the singleton doesn't keep us pinned.
     */
    private function registerShutdownEmit(): void
    {
        if ($this->shutdownRegistered) {
            return;
        }
        $this->shutdownRegistered = true;
        $weak = \WeakReference::create($this);
        register_shutdown_function(static function () use ($weak) {
            $self = $weak->get();
            if ($self !== null) {
                try {
                    $self->emitWrapperDisconnected();
                } catch (\Throwable $e) {
                    // Shutdown-time errors must not abort the process.
                }
            }
        });
    }

    private static function generateUuidV4(): string
    {
        $bytes = random_bytes(16);
        // Set version (4) + variant (10xx) bits per RFC 4122.
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
        $hex = bin2hex($bytes);
        return sprintf(
            '%s-%s-%s-%s-%s',
            substr($hex, 0, 8),
            substr($hex, 8, 4),
            substr($hex, 12, 4),
            substr($hex, 16, 4),
            substr($hex, 20, 12),
        );
    }

    /**
     * Resolve the wrapper version from composer.json (walking up from
     * this file's location). Falls back to "unknown" if the manifest
     * isn't found or doesn't declare a version. We avoid hardcoding
     * because Packagist serves whatever the git tag declares, not the
     * manifest, so a hardcoded version drifts from the tag at release
     * time.
     */
    private static function resolveWrapperVersion(): string
    {
        // composer.json sits at the package root; this file is at
        // src/NativeCache.php. Walk up two directories.
        $manifest = dirname(__DIR__) . '/composer.json';
        if (!is_file($manifest)) {
            return 'unknown';
        }
        $raw = @file_get_contents($manifest);
        if ($raw === false) {
            return 'unknown';
        }
        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) {
            return 'unknown';
        }
        $version = $decoded['version'] ?? null;
        if (is_string($version) && $version !== '') {
            return $version;
        }
        return 'unknown';
    }

    /**
     * Decide whether telemetry should be sent on the wire.
     *
     * - Explicit GOLDLAPEL_REPORT_STATS=true|false wins.
     * - Otherwise, default ON only under the CLI SAPI. Short-lived
     *   request-handling SAPIs (fpm-fcgi, cgi-fcgi, apache2handler,
     *   etc.) connect, do one query, and exit — telemetry would emit
     *   wrapper_connected/wrapper_disconnected on every request and
     *   flood the proxy. The cache itself continues to function in
     *   those SAPIs; only S:/R: emissions are suppressed.
     */
    private static function resolveReportStats(): bool
    {
        $env = getenv('GOLDLAPEL_REPORT_STATS');
        if ($env !== false && $env !== '') {
            return strtolower($env) !== 'false';
        }
        return PHP_SAPI === 'cli' || PHP_SAPI === 'phpdbg';
    }

    private static function encodeJson(array $payload): ?string
    {
        $encoded = json_encode($payload, JSON_UNESCAPED_SLASHES);
        if ($encoded === false) {
            return null;
        }
        return $encoded;
    }
}
