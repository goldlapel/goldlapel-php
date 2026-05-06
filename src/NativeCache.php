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

    // ---- Unsafe-GUC classification (Option Y — mirrors proxy `src/guc_state.rs`) ----
    //
    // Custom-GUC-driven RLS (`SET app.user_id = '42'; SELECT * FROM accounts;`
    // where the policy reads `current_setting('app.user_id')`) is a real
    // cache leak: keying purely by SQL+params, user A's cached rows could
    // be served to user B. The wrapper folds a per-connection unsafe-GUC
    // fingerprint into the cache key — see ConnectionGucState for the
    // per-connection mutable state. NativeCache only owns the
    // classification rules (the `is_unsafe_guc` short list + namespace
    // `.` rule) plus the SET/RESET parser primitives, both static.
    //
    // A GUC name is unsafe if it's in this short hardcoded list OR if it
    // contains a `.` (namespaced — `app.*`, `myapp.*`, the canonical
    // custom-RLS pattern). Comparison is case-insensitive.
    //
    // `SET LOCAL` is observed-but-ignored by ConnectionGucState: the
    // cache is gated on transaction-idle, so SET LOCAL effects never
    // influence a cacheable response.
    // Expanded 2026-05-05: locale + formatting GUCs that can plausibly
    // change query output shape (DateStyle/IntervalStyle change the text
    // representation of timestamps; TimeZone shifts wall-clock conversions
    // for `now()`, `current_timestamp`, etc.; bytea_output changes the
    // text rendering of bytea columns; lc_* affect locale-sensitive
    // collation / monetary / numeric formatting). None of these are
    // common RLS pivots, but they're cheap to track and correctly avoid
    // serving cached rows whose textual representation no longer matches
    // the caller's session expectations.
    private const UNSAFE_GUC_SHORT_LIST = [
        'search_path',
        'role',
        'session_authorization',
        'default_transaction_isolation',
        'default_transaction_read_only',
        'transaction_isolation',
        'row_security',
        'datestyle',
        'intervalstyle',
        'timezone',
        'bytea_output',
        'lc_messages',
        'lc_monetary',
        'lc_numeric',
        'lc_time',
    ];

    // --- Native-cache telemetry tuning ---
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
    // disable_native_cache: when true the cache acts as a no-op pass-through.
    // Distinct from $enabled (the env-var/legacy toggle) — disabled() ticks
    // misses on every get() so the proxy can still see the wrapper's traffic
    // shape via telemetry, while $enabled=false (cache_size=0 family) goes
    // fully dark. Invalidation polling + state-change emissions continue to
    // run regardless, so the proxy's telemetry path is unaffected.
    private bool $disabled;
    private bool $invalidationConnected = false;
    private int $invalidationPort = 0;
    /** @var resource|null */
    private $socket = null;
    private bool $stopped = false;

    public int $statsHits = 0;
    public int $statsMisses = 0;
    public int $statsInvalidations = 0;
    // Native-cache telemetry — was missing before; bumped in evictOne().
    // Public so tests + external integrations can read it the same way they
    // read the other counters.
    public int $statsEvictions = 0;

    // Native-cache telemetry state. wrapperId is generated once per process
    // and stable across reconnects; lets the proxy aggregate per-wrapper.
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

    public function __construct(?int $maxEntries = null, ?bool $enabled = null, bool $disabled = false)
    {
        $this->maxEntries = $maxEntries ?? (int) (getenv('GOLDLAPEL_NATIVE_CACHE_SIZE') ?: '32768');
        $envEnabled = getenv('GOLDLAPEL_NATIVE_CACHE');
        $this->enabled = $enabled ?? ($envEnabled === false || strtolower($envEnabled) !== 'false');
        $this->disabled = $disabled;
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

    /**
     * Toggle the native-cache no-op pass-through. When true, get() returns
     * null and ticks misses, put() is a silent no-op, and the
     * wrapper_connected snapshot reports `disabled: true`. Invalidation
     * polling continues either way so telemetry stays live. Used by the
     * canonical `disable_native_cache` startup option — the singleton is
     * configured by GoldLapel::wrapPDO()/wrapPDOStatic() before the first
     * cache op.
     */
    public function setDisabled(bool $disabled): void
    {
        $this->disabled = $disabled;
    }

    public function isDisabled(): bool
    {
        return $this->disabled;
    }

    public function size(): int
    {
        return count($this->cache);
    }

    // --- Cache operations ---

    public function get(string $sql, ?array $params = null, string $stateHash = '0'): ?array
    {
        if (!$this->enabled || !$this->invalidationConnected) {
            return null;
        }
        // disable_native_cache: tick misses (so the proxy still sees the
        // request rate through telemetry) but never serve a hit. Hits +
        // evictions stay at zero; put() is a sibling no-op below.
        if ($this->disabled) {
            $this->statsMisses++;
            return null;
        }
        $key = self::makeKey($sql, $params, $stateHash);
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

    public function put(string $sql, ?array $params, array $rows, ?array $columns, string $stateHash = '0'): void
    {
        if (!$this->enabled || !$this->invalidationConnected) {
            return;
        }
        if ($this->disabled) {
            return;
        }
        $key = self::makeKey($sql, $params, $stateHash);
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

    /**
     * Build a deterministic cache key from sql + bind params + the
     * connection's unsafe-GUC state hash. The state hash is folded in
     * unconditionally; for connections that haven't run any unsafe SET,
     * the hash is `"0"` and shared with peer connections — so a shared
     * slot still works for the SET-free majority.
     */
    public static function makeKey(string $sql, ?array $params = null, string $stateHash = '0'): ?string
    {
        $base = $stateHash . "\0" . $sql;
        if ($params === null || empty($params)) {
            return $base . "\0null";
        }
        return $base . "\0" . serialize($params);
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
                // Re-tokenize from a literal-stripped form so that bare
                // words like `INTO` or `FROM` inside `'...'` / `"..."`
                // don't trigger the SELECT-INTO DDL classifier (e.g.
                // `SELECT 'INSERT INTO orders' FROM audit_log`,
                // `SELECT * FROM "into_table"`). Mirrors goldlapel-js
                // commit 63753fe.
                $scanTokens = preg_split('/\s+/', self::stripStringLiterals($trimmed));
                $sawInto = false;
                $intoTarget = null;
                for ($i = 1; $i < count($scanTokens); $i++) {
                    $upper = strtoupper($scanTokens[$i]);
                    if ($upper === 'INTO' && !$sawInto) {
                        $sawInto = true;
                        continue;
                    }
                    if ($sawInto && $intoTarget === null) {
                        if (in_array($upper, ['TEMPORARY', 'TEMP', 'UNLOGGED'], true)) {
                            continue;
                        }
                        $intoTarget = $scanTokens[$i];
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

    /**
     * Multi-statement-aware write detection. Reuses splitStatements so a
     * single Q message like `SET app.user_id = '42'; INSERT INTO orders
     * VALUES (1)` doesn't slip past detectWrite's first-token check —
     * which would otherwise see `SET`, return null, and let the INSERT
     * run on the server while the cached `SELECT * FROM orders` slot
     * survives. Mirrors the JS wrap.js detectWritesMulti shape.
     *
     * Returns:
     *   - DDL_SENTINEL  if any segment is DDL (caller invalidateAll's)
     *   - list<string>  of unique table names if any segments are
     *                   table-scoped writes
     *   - null          if no segment is a write
     *
     * Single-statement bodies skip the splitter entirely (hot path).
     *
     * @return string|list<string>|null
     */
    public static function detectWritesMulti(string $sql): string|array|null
    {
        if ($sql === '') {
            return null;
        }
        // Fast path: no inner `;` → single-statement, original detectWrite.
        $tail = rtrim($sql);
        if (str_ends_with($tail, ';')) {
            $tail = rtrim(substr($tail, 0, -1));
        }
        if (!str_contains($tail, ';')) {
            $t = self::detectWrite($sql);
            if ($t === null) {
                return null;
            }
            if ($t === self::DDL_SENTINEL) {
                return self::DDL_SENTINEL;
            }
            return [$t];
        }
        $tables = [];
        foreach (self::splitStatements($sql) as $seg) {
            $t = self::detectWrite($seg);
            if ($t === self::DDL_SENTINEL) {
                return self::DDL_SENTINEL;
            }
            if ($t !== null) {
                $tables[$t] = true;
            }
        }
        return empty($tables) ? null : array_keys($tables);
    }

    /**
     * Postgres SQL-prefix commands whose response is a session-state /
     * control-flow signal rather than a cacheable read. Caching their
     * empty-row reply bloats the cache with no-row entries that never
     * serve real data and triggers needless eviction pressure.
     *
     * The async-libpq path (PostgresResult.getColumnCount() === null)
     * already filters most of these out, but the sync PDO::query()
     * path has no equivalent column-count signal, so we gate on the
     * SQL prefix instead. Mirrors JS NON_CACHEABLE_COMMANDS.
     */
    private const NON_CACHEABLE_COMMAND_TOKENS = [
        'SET', 'RESET', 'LISTEN', 'UNLISTEN', 'NOTIFY',
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'END',
        'START', // START TRANSACTION
        'RELEASE', // RELEASE SAVEPOINT
    ];

    /**
     * True if the SQL's first token is one of the
     * NON_CACHEABLE_COMMAND_TOKENS. Used as a put-side guard so
     * `query("SET app.user_id = '7'")` doesn't pollute the cache with
     * an empty-row entry keyed under the SET text.
     */
    public static function isNonCacheableCommand(string $sql): bool
    {
        $trimmed = ltrim($sql);
        if ($trimmed === '') {
            return false;
        }
        $tokens = preg_split('/\s+/', $trimmed, 2);
        if ($tokens === false || $tokens[0] === '') {
            return false;
        }
        $first = strtoupper($tokens[0]);
        return in_array($first, self::NON_CACHEABLE_COMMAND_TOKENS, true);
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

    /**
     * Multi-statement-aware transaction-boundary classification. A single
     * Q message like `BEGIN; LISTEN foo; COMMIT` opens AND closes a
     * transaction server-side, but a first-token check (`isTxStart($sql)`)
     * only sees `BEGIN` and would leave the wrapper stuck thinking it was
     * still in a tx — so subsequent reads bypass the cache forever (or
     * until a fresh BEGIN/COMMIT cycle resets state). The fix walks every
     * segment and applies the boundary that segment carries, so the
     * caller's final tx flag matches what the server actually did.
     *
     * Returns:
     *   - true   if the body's final segment-walked tx state is `in tx`
     *   - false  if the body's final segment-walked tx state is `out of tx`
     *   - null   if no segment is a tx boundary (caller's state unchanged)
     *
     * Single-statement bodies (no inner `;`) skip the splitter — the
     * existing TX_START / TX_END regex check is sufficient and the hot
     * path is hot. Mirrors goldlapel-js commit 0d19816.
     */
    public static function applyTxBoundaries(string $sql): ?bool
    {
        if ($sql === '') {
            return null;
        }
        // Fast path: no inner `;` → single-statement, original first-token check.
        $tail = rtrim($sql);
        if (str_ends_with($tail, ';')) {
            $tail = rtrim(substr($tail, 0, -1));
        }
        if (!str_contains($tail, ';')) {
            if (self::isTxStart($sql)) {
                return true;
            }
            if (self::isTxEnd($sql)) {
                return false;
            }
            return null;
        }
        // Multi-statement body: walk segments, track the running tx state.
        // Final state reflects the last tx-boundary segment in execution
        // order, mirroring how the server processes the Q message.
        $touched = false;
        $state = false;
        foreach (self::splitStatements($sql) as $seg) {
            if (self::isTxStart($seg)) {
                $state = true;
                $touched = true;
            } elseif (self::isTxEnd($seg)) {
                $state = false;
                $touched = true;
            }
        }
        return $touched ? $state : null;
    }

    // ------------------------------------------------------------------
    // Unsafe-GUC state hash (Option Y — wrapper-side mirror of the proxy)
    // ------------------------------------------------------------------

    /**
     * Classify a GUC name as state-affecting (true) or harmless (false).
     * Unsafe if name is in the short hardcoded list OR contains a `.`
     * (namespaced — `app.*`, `myapp.*`, the canonical custom-RLS
     * pattern). Comparison is case-insensitive.
     */
    public static function isUnsafeGuc(string $name): bool
    {
        $lower = strtolower($name);
        if (str_contains($lower, '.')) {
            return true;
        }
        return in_array($lower, self::UNSAFE_GUC_SHORT_LIST, true);
    }

    /**
     * Replace the contents of `'...'` and `"..."` string literals with
     * spaces, preserving overall length so positions line up with the
     * original. PG's doubled-quote `''` / `""` escapes are handled the
     * same way as in splitStatements. Used by detectWrite's SELECT branch
     * so that bare words like `INTO` inside a literal (e.g.
     * `SELECT 'INSERT INTO orders' FROM audit_log`) don't trip the
     * SELECT-INTO DDL classifier. Mirrors goldlapel-js commit 63753fe.
     */
    public static function stripStringLiterals(string $sql): string
    {
        $len = strlen($sql);
        if ($len === 0) {
            return $sql;
        }
        $out = $sql;
        $quote = null;
        $i = 0;
        while ($i < $len) {
            $c = $sql[$i];
            if ($quote !== null) {
                if ($c === $quote) {
                    if ($i + 1 < $len && $sql[$i + 1] === $quote) {
                        // Doubled-quote escape: blank both, stay inside literal.
                        $out[$i] = ' ';
                        $out[$i + 1] = ' ';
                        $i += 2;
                        continue;
                    }
                    // Closing quote: leave the delimiter, drop the literal body.
                    $quote = null;
                } else {
                    $out[$i] = ' ';
                }
            } else {
                if ($c === "'" || $c === '"') {
                    $quote = $c;
                }
            }
            $i++;
        }
        return $out;
    }

    /**
     * Split a SQL string on top-level `;`, respecting single- and
     * double-quoted string literals (PG `''` and `""` doubled-quote
     * escapes handled). Returns trimmed segments, dropping empty ones.
     *
     * Lightest-possible splitter — does not understand dollar-quoted
     * strings, comments, or anything else lexical. Good enough for
     * splitting `SET foo = 'a'; SELECT 1`-style multi-statement Q
     * bodies, which is the entire reason it exists.
     *
     * @return list<string>
     */
    public static function splitStatements(string $sql): array
    {
        $out = [];
        $start = 0;
        $quote = null;
        $i = 0;
        $len = strlen($sql);
        while ($i < $len) {
            $c = $sql[$i];
            if ($quote !== null) {
                if ($c === $quote) {
                    // PG `''` and SQL-standard `""` doubled-quote escapes.
                    if ($i + 1 < $len && $sql[$i + 1] === $quote) {
                        $i += 2;
                        continue;
                    }
                    $quote = null;
                }
            } else {
                if ($c === "'" || $c === '"') {
                    $quote = $c;
                } elseif ($c === ';') {
                    $segment = trim(substr($sql, $start, $i - $start));
                    if ($segment !== '') {
                        $out[] = $segment;
                    }
                    $start = $i + 1;
                }
            }
            $i++;
        }
        $tail = trim(substr($sql, $start));
        if ($tail !== '') {
            $out[] = $tail;
        }
        return $out;
    }

    /**
     * Parse a single SET / RESET command out of one SQL statement. Returns
     * an array describing the parsed shape, or null if the statement isn't
     * one of the recognised forms.
     *
     * Recognised forms:
     *   * `SET name = value`, `SET name TO value`
     *   * `SET SESSION name = value`, `SET SESSION name TO value`
     *   * `SET LOCAL name = value`, `SET LOCAL name TO value`
     *   * `RESET name`
     *   * `RESET ALL`
     *
     * Return shape (or null):
     *   ['type' => 'set' | 'set_local' | 'reset' | 'reset_all',
     *    'name' => string|null, 'value' => string|null]
     *
     * Anything else (including `SET TIME ZONE ...`) returns null —
     * timezone is harmless and the unusual two-word GUC name doesn't
     * fit this pattern.
     *
     * @return array{type: string, name: ?string, value: ?string}|null
     */
    public static function parseSetCommand(string $sql): ?array
    {
        // Trim whitespace + a single trailing semicolon.
        $s = trim($sql);
        if (str_ends_with($s, ';')) {
            $s = rtrim(substr($s, 0, -1));
        }
        if ($s === '') {
            return null;
        }

        $tokens = preg_split('/\s+/', $s);
        if ($tokens === false || count($tokens) === 0) {
            return null;
        }
        $head = $tokens[0];

        if (strcasecmp($head, 'RESET') === 0) {
            if (!isset($tokens[1])) {
                return null;
            }
            $target = $tokens[1];
            // `RESET name` — anything after `name` is junk we don't expect.
            if (isset($tokens[2])) {
                return null;
            }
            if (strcasecmp($target, 'ALL') === 0) {
                return ['type' => 'reset_all', 'name' => null, 'value' => null];
            }
            $name = self::normalizeGucName($target);
            if ($name === null) {
                return null;
            }
            return ['type' => 'reset', 'name' => $name, 'value' => null];
        }

        if (strcasecmp($head, 'SET') !== 0) {
            return null;
        }

        // Optional `LOCAL` / `SESSION` modifier.
        if (!isset($tokens[1])) {
            return null;
        }
        $next = $tokens[1];
        $cursor = 2;
        $isLocal = false;
        if (strcasecmp($next, 'LOCAL') === 0) {
            if (!isset($tokens[$cursor])) {
                return null;
            }
            $next = $tokens[$cursor];
            $cursor++;
            $isLocal = true;
        } elseif (strcasecmp($next, 'SESSION') === 0) {
            if (!isset($tokens[$cursor])) {
                return null;
            }
            $next = $tokens[$cursor];
            $cursor++;
        }

        // The name token may have an `=` glued onto it (e.g.
        // `SET app.user='42'`). Split on `=` if present.
        $gluedValue = null;
        $eqPos = strpos($next, '=');
        if ($eqPos !== false) {
            $nameToken = substr($next, 0, $eqPos);
            $rest = substr($next, $eqPos + 1);
            $gluedValue = $rest === '' ? null : $rest;
        } else {
            $nameToken = $next;
        }

        $name = self::normalizeGucName($nameToken);
        if ($name === null) {
            return null;
        }

        if ($gluedValue !== null) {
            $tail = array_slice($tokens, $cursor);
            $valueStr = empty($tail) ? $gluedValue : $gluedValue . ' ' . implode(' ', $tail);
        } else {
            if (!isset($tokens[$cursor])) {
                return null;
            }
            $sep = $tokens[$cursor];
            $cursor++;
            if (!($sep === '=' || strcasecmp($sep, 'TO') === 0)) {
                return null;
            }
            $valueStr = implode(' ', array_slice($tokens, $cursor));
        }

        $value = self::stripValueQuotes(trim($valueStr));
        if ($value === '' && trim($valueStr) === '') {
            return null;
        }

        return [
            'type' => $isLocal ? 'set_local' : 'set',
            'name' => $name,
            'value' => $value,
        ];
    }

    /**
     * Lowercase the GUC name and strip surrounding double quotes.
     */
    private static function normalizeGucName(string $token): ?string
    {
        $trimmed = trim($token, '"');
        if ($trimmed === '') {
            return null;
        }
        return strtolower($trimmed);
    }

    /**
     * Strip a single layer of matching surrounding quotes (`'...'` or
     * `"..."`) from a value. Multi-token quoted values arrive joined
     * already; this peels the outer quotes. Unquoted values are
     * returned trimmed.
     */
    private static function stripValueQuotes(string $value): string
    {
        $v = trim($value);
        if (strlen($v) >= 2) {
            $first = $v[0];
            $last = $v[strlen($v) - 1];
            if (($first === "'" && $last === "'") || ($first === '"' && $last === '"')) {
                return substr($v, 1, -1);
            }
        }
        return $v;
    }

    // Per-connection unsafe-GUC state lives on ConnectionGucState; see
    // src/ConnectionGucState.php. NativeCache only owns the
    // classification rules (isUnsafeGuc, parseSetCommand,
    // splitStatements) above — they're pure functions.

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
    // Native-cache telemetry
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
     * Build the native-cache snapshot dict the proxy aggregates per-tick. All
     * counters + cache size read in a single pass for internal
     * consistency. Public so external dashboards can call it directly.
     */
    public function buildSnapshot(): array
    {
        $snap = [
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
        // Surfaced only when set, so existing snapshot consumers keep their
        // current key shape and the proxy can detect disabled wrappers
        // without an extra version negotiation.
        if ($this->disabled) {
            $snap['disabled'] = true;
        }
        return $snap;
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
