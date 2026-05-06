<?php

namespace GoldLapel;

class CachedPDO extends \PDO
{
    private \PDO $pdo;
    private NativeCache $cache;
    /**
     * Per-connection unsafe-GUC state tracker. Each CachedPDO has its own
     * — sharing it across connections would let one connection's `SET
     * app.user_id` poison another's cache reads. See ConnectionGucState
     * for the security rationale.
     */
    private ConnectionGucState $gucState;
    private bool $inTransaction = false;

    // Extends PDO so instanceof checks and type hints work.
    // We intentionally skip parent::__construct() to avoid opening a
    // second connection — all calls delegate to the wrapped $pdo.
    public function __construct(\PDO $pdo, NativeCache $cache)
    {
        // Do NOT call parent::__construct() — we delegate to $pdo instead.
        $this->pdo = $pdo;
        $this->cache = $cache;
        $this->gucState = new ConnectionGucState();
    }

    public function getWrappedPDO(): \PDO
    {
        return $this->pdo;
    }

    public function unwrap(): \PDO
    {
        return $this->pdo;
    }

    public function getCache(): NativeCache
    {
        return $this->cache;
    }

    public function getGucState(): ConnectionGucState
    {
        return $this->gucState;
    }

    /**
     * Run a server-side verify pass if (and only if) the connection is
     * marked dirty. Hands the result of
     *   SELECT name, setting FROM pg_settings WHERE source='session'
     * to ConnectionGucState::applyVerifyResult(), which rebuilds the
     * unsafe-GUC subset and clears the dirty flag.
     *
     * Calling pattern: the wrapper invokes this lazily on every
     * acquire / reuse point. Persistent PDO connections (PDO::ATTR_PERSISTENT)
     * are the closest PHP analog to a connection pool — when the same
     * underlying PDO handle is handed back to a new request, a previous
     * request might have stored-function-SET an `app.user_id` we never
     * saw. Verify-on-reuse is the universal fallback that closes that
     * gap regardless of whether the underlying PDO is persistent.
     *
     * Idempotent — calling on a clean connection is free (just a flag
     * read). On verify-query failure, the connection stays dirty so the
     * NEXT acquire path retries; the user's query is never blocked or
     * errored by our verify failure.
     *
     * Returns true if a verify ran successfully, false otherwise (clean
     * connection, or verify query failed).
     */
    public function verifyIfDirty(): bool
    {
        if (!$this->gucState->isDirty()) {
            return false;
        }
        try {
            $stmt = $this->pdo->query(
                "SELECT name, setting FROM pg_settings WHERE source='session'"
            );
            if ($stmt === false) {
                return false;
            }
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            if (!is_array($rows)) {
                return false;
            }
            $this->gucState->applyVerifyResult($rows);
            return true;
        } catch (\Throwable $e) {
            // Verify failure is silent — the connection stays dirty,
            // the next acquire retries. We never propagate to the
            // caller because the user's query hasn't even started yet.
            return false;
        }
    }

    /**
     * Force the connection into the dirty state, so the next
     * verifyIfDirty() call will run the server-side verify pass.
     * Exposed for the persistent-PDO reuse path: a frontend that
     * hands a wrapped persistent PDO back to a new request can
     * call markStateDirty() to ensure the next query path
     * re-syncs unsafe-GUC state.
     */
    public function markStateDirty(): void
    {
        $this->gucState->markDirty();
    }

    /**
     * Post-call verify for top-level `SELECT <function>(...)` shapes.
     * Stored functions can issue `SET app.user_id = ...` server-side,
     * which the wire-side observeSql() never sees. After capturing the
     * user's result, we run the verify-on-checkout query inline (sync
     * PHP has no native off-thread path — Amp users get the async
     * variant via Amp\CachedConnection). On verify failure we mark the
     * connection dirty so the NEXT acquire path retries; user's data
     * has already been captured and is unaffected.
     *
     * Tax: ~1 round-trip to the proxy on every recognised function-call
     * statement. Documented in CachedPDO::query() — sync PHP can't
     * defer this without breaking the Promise model the user expects.
     *
     * Visibility is `public` (rather than private) so CachedPDOStatement
     * can invoke it after a prepare+execute path runs a top-level
     * function call — the statement on its own has no PDO handle.
     */
    public function postCallVerify(string $sql): void
    {
        if (!NativeCache::isTopLevelFunctionCall($sql)) {
            return;
        }
        try {
            $stmt = $this->pdo->query(
                "SELECT name, setting FROM pg_settings WHERE source='session'"
            );
            if ($stmt === false) {
                $this->gucState->markDirty();
                return;
            }
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
            if (!is_array($rows)) {
                $this->gucState->markDirty();
                return;
            }
            $this->gucState->applyVerifyResult($rows);
        } catch (\Throwable $e) {
            // Mark dirty so the NEXT acquire path retries the verify.
            // Never propagate — the user's data is already captured.
            $this->gucState->markDirty();
        }
    }

    public function query(string $sql, ...$args): CachedPDOStatement|false
    {
        // Verify-on-checkout fallback: if a previous code path marked
        // the connection dirty (e.g. persistent PDO reuse, an earlier
        // post-call verify failure), reconstruct the unsafe-GUC state
        // from pg_settings before the read path uses the state hash.
        // No-op on clean connections.
        $this->verifyIfDirty();

        // Observe unsafe-GUC SET / RESET before any other handling so the
        // state hash reflects the effective post-statement state when the
        // result is keyed (matters for `SET app.user_id = '7'; SELECT ...`
        // multi-statements). State lives on this CachedPDO so concurrent
        // connections never share gucState.
        $this->gucState->observeSql($sql);

        // Multi-statement-aware write detection. Runs BEFORE transaction
        // tracking so a single Q message like `BEGIN; INSERT INTO orders
        // VALUES (1); COMMIT` still surfaces the INSERT for invalidation
        // — TX_START's first-token check would otherwise swallow the
        // whole body as a transaction-boundary marker and the INSERT
        // would never invalidate the `orders` cache slot. Same gap fixed
        // for `SET app.tenant = 'x'; INSERT INTO t ...` (SET hides the
        // INSERT from detectWrite's single-token shape).
        $writeTables = NativeCache::detectWritesMulti($sql);
        if ($writeTables !== null) {
            if ($writeTables === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                foreach ($writeTables as $t) {
                    $this->cache->invalidateTable($t);
                }
            }
            $stmt = $this->pdo->query($sql, ...$args);
            $this->postCallVerify($sql);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction, $this) : false;
        }

        // Transaction tracking. Walks every segment in multi-statement
        // bodies so a Q like `BEGIN; LISTEN foo; COMMIT` ends with
        // inTransaction=false — matching the server, which ran the
        // COMMIT. Pre-fix, only the leading BEGIN was seen and the
        // wrapper stayed stuck thinking a tx was open, bypassing the
        // cache for every subsequent read until the next BEGIN/COMMIT
        // pair reset state. Single-statement bodies skip the splitter.
        $txState = NativeCache::applyTxBoundaries($sql);
        if ($txState !== null) {
            $this->inTransaction = $txState;
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction, $this) : false;
        }

        // Inside transaction: bypass cache. Post-call verify is also
        // skipped — server-side state mutations made inside an open
        // transaction won't affect another connection's cache reads
        // until the tx commits, and the wrapper's own state hash
        // doesn't matter while inTransaction=true (cache is bypassed
        // anyway). Verify will be triggered as needed once the tx ends
        // and the next read runs.
        if ($this->inTransaction) {
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, true, $this) : false;
        }

        // Skip the cache entirely for session-state commands (SET / RESET /
        // LISTEN / etc). PDO::query("SET app.user_id = '7'") would otherwise
        // fetchAll an empty row set and put it in the cache, bloating the
        // table with no-row entries that never serve real data and
        // triggering needless eviction pressure on session-heavy
        // workloads. The prepare/execute path is gated by columnCount() > 0
        // already, so this only catches the direct-query path.
        if (NativeCache::isNonCacheableCommand($sql)) {
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction, $this) : false;
        }

        // Read path: check native cache, keyed on this connection's GUC fingerprint.
        $stateHash = $this->gucState->stateHash();
        $entry = $this->cache->get($sql, null, $stateHash);
        if ($entry !== null) {
            // Cache hit: function body did NOT execute server-side, so
            // there's no new state to verify. Return without verify.
            return CachedPDOStatement::fromCache($entry, $this->cache, $this->gucState, $sql);
        }

        // Cache miss: execute for real
        $stmt = $this->pdo->query($sql, ...$args);
        if ($stmt === false) {
            // Even on failure, a top-level function call may have
            // partially executed before erroring out. Mark dirty so
            // the next acquire reverifies — better than assuming the
            // failed call was atomic.
            if (NativeCache::isTopLevelFunctionCall($sql)) {
                $this->gucState->markDirty();
            }
            return false;
        }

        // Cache the result under this connection's state hash.
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $columns = !empty($rows) ? array_keys($rows[0]) : [];
        $this->cache->put($sql, null, $rows, $columns, $stateHash);

        // Post-call verify for top-level SELECT <function>(...). Synchronous
        // in PHP — no off-thread option without Amp. ~1ms tax per call;
        // documented in postCallVerify's docblock. Result data is already
        // captured into $rows, so verify failure only marks dirty for the
        // next acquire — user's response is unaffected.
        $this->postCallVerify($sql);

        return CachedPDOStatement::fromCache(
            ['rows' => $rows, 'columns' => $columns, 'tables' => NativeCache::extractTables($sql)],
            $this->cache,
            $this->gucState,
            $sql
        );
    }

    public function prepare(string $sql, array $options = []): CachedPDOStatement
    {
        // Verify-on-checkout fallback runs at prepare-time so the
        // execute() path's state hash is already trustworthy.
        $this->verifyIfDirty();
        $realStmt = $this->pdo->prepare($sql, $options);
        return new CachedPDOStatement(
            $realStmt,
            $this->cache,
            $this->gucState,
            $sql,
            null,
            fn() => $this->inTransaction,
            $this,
        );
    }

    public function exec(string $sql): int|false
    {
        // Verify-on-checkout fallback (mirrors query()).
        $this->verifyIfDirty();

        // Observe unsafe-GUC SET / RESET before write detection. Mirrors
        // query(); a `SET app.user_id = '7'` issued via exec() must
        // update this connection's state hash so subsequent reads on the
        // same CachedPDO key correctly.
        $this->gucState->observeSql($sql);

        // Multi-statement-aware write detection. `exec()` doesn't return a
        // result set, so we can't fall through to a read path — but a
        // multi-statement body still needs every write surfaced for
        // invalidation (e.g. `SET app.tenant = 'x'; INSERT INTO orders
        // VALUES (1)` would otherwise let the orders cache survive).
        $writeTables = NativeCache::detectWritesMulti($sql);
        if ($writeTables !== null) {
            if ($writeTables === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                foreach ($writeTables as $t) {
                    $this->cache->invalidateTable($t);
                }
            }
        }

        // Transaction tracking. Walks every segment in multi-statement
        // bodies so a Q like `BEGIN; LISTEN foo; COMMIT` ends with
        // inTransaction=false — matching the server. Single-statement
        // bodies skip the splitter.
        $txState = NativeCache::applyTxBoundaries($sql);
        if ($txState !== null) {
            $this->inTransaction = $txState;
        }

        $result = $this->pdo->exec($sql);
        // Post-call verify if this exec was a top-level function call
        // (`SELECT my_handler()` issued via exec). Skip inside a tx —
        // see CachedPDO::query() for the rationale.
        if (!$this->inTransaction && $result !== false) {
            $this->postCallVerify($sql);
        } elseif ($result === false && NativeCache::isTopLevelFunctionCall($sql)) {
            $this->gucState->markDirty();
        }
        return $result;
    }

    public function beginTransaction(): bool
    {
        $this->inTransaction = true;
        return $this->pdo->beginTransaction();
    }

    public function commit(): bool
    {
        $this->inTransaction = false;
        return $this->pdo->commit();
    }

    public function rollBack(): bool
    {
        $this->inTransaction = false;
        return $this->pdo->rollBack();
    }

    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    public function lastInsertId(?string $name = null): string|false
    {
        return $this->pdo->lastInsertId($name);
    }

    public function quote(string $string, int $type = \PDO::PARAM_STR): string|false
    {
        return $this->pdo->quote($string, $type);
    }

    public function setAttribute(int $attribute, mixed $value): bool
    {
        return $this->pdo->setAttribute($attribute, $value);
    }

    public function getAttribute(int $attribute): mixed
    {
        return $this->pdo->getAttribute($attribute);
    }

    public function errorCode(): ?string
    {
        return $this->pdo->errorCode();
    }

    public function errorInfo(): array
    {
        return $this->pdo->errorInfo();
    }

    public function __call(string $method, array $args): mixed
    {
        return $this->pdo->$method(...$args);
    }
}

class CachedPDOStatement extends \PDOStatement
{
    private ?\PDOStatement $realStmt;
    private NativeCache $cache;
    /**
     * Per-connection GUC state, shared by-reference with the parent
     * CachedPDO. Statements observe SET / RESET so a `SET app.user_id = …`
     * issued through `prepare`+`execute` updates the connection's state,
     * not the statement's — that's what ConnectionGucState's
     * `observeSql()` does (the object holds the state).
     */
    private ConnectionGucState $gucState;
    private string $sql;
    private ?array $params;
    private \Closure $inTransaction;
    /**
     * Optional reference to the parent CachedPDO. Used by execute() to
     * trigger postCallVerify() after a top-level `SELECT <function>(...)`
     * via prepare+execute — the statement on its own can't issue the
     * pg_settings query without going back through the PDO. Null when
     * the statement was constructed from cache (fromCache(), no PDO
     * involved).
     */
    private ?CachedPDO $parent;

    private ?array $cachedRows = null;
    private ?array $cachedColumns = null;
    private int $fetchIndex = 0;
    private bool $fromCache = false;

    public function __construct(
        ?\PDOStatement $realStmt,
        NativeCache $cache,
        ConnectionGucState $gucState,
        string $sql,
        ?array $params = null,
        bool|\Closure $inTransaction = false,
        ?CachedPDO $parent = null,
    ) {
        $this->realStmt = $realStmt;
        $this->cache = $cache;
        $this->gucState = $gucState;
        $this->sql = $sql;
        $this->params = $params;
        if ($inTransaction instanceof \Closure) {
            $this->inTransaction = $inTransaction;
        } else {
            $val = $inTransaction;
            $this->inTransaction = static function () use ($val) { return $val; };
        }
        $this->parent = $parent;
    }

    public static function fromCache(array $entry, NativeCache $cache, ConnectionGucState $gucState, string $sql): self
    {
        $stmt = new self(null, $cache, $gucState, $sql);
        $stmt->cachedRows = $entry['rows'];
        $stmt->cachedColumns = $entry['columns'];
        $stmt->fetchIndex = 0;
        $stmt->fromCache = true;
        return $stmt;
    }

    public function execute(?array $params = null): bool
    {
        $effectiveParams = $params ?? $this->params;
        $this->cachedRows = null;
        $this->cachedColumns = null;
        $this->fetchIndex = 0;
        $this->fromCache = false;

        // Observe unsafe-GUC SET / RESET. Prepared `SET app.user_id = $1`
        // is unusual but legal; observeSql() captures the placeholder
        // string `$1` as the recorded value, which won't match the
        // proxy's per-bind hash — at worst the wrapper sees cache misses
        // for that connection's reads. The proxy still keys correctly
        // because it sees the bound value on the wire, so this is
        // never a leak.
        $this->gucState->observeSql($this->sql);

        // Multi-statement-aware write detection. Runs BEFORE transaction
        // tracking — see CachedPDO::query() for the reasoning.
        $writeTables = NativeCache::detectWritesMulti($this->sql);
        if ($writeTables !== null) {
            if ($writeTables === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                foreach ($writeTables as $t) {
                    $this->cache->invalidateTable($t);
                }
            }
            $r = $this->realStmt->execute($params);
            $this->maybePostCallVerify($r);
            return $r;
        }

        // Transaction tracking
        if (NativeCache::isTxStart($this->sql)) {
            return $this->realStmt->execute($params);
        }
        if (NativeCache::isTxEnd($this->sql)) {
            return $this->realStmt->execute($params);
        }

        // Inside transaction: bypass cache. Post-call verify is also
        // skipped — see CachedPDO::query() for the rationale.
        if (($this->inTransaction)()) {
            return $this->realStmt->execute($params);
        }

        // Read path: check native cache, keyed on this connection's GUC fingerprint.
        $stateHash = $this->gucState->stateHash();
        $entry = $this->cache->get($this->sql, $effectiveParams, $stateHash);
        if ($entry !== null) {
            // Cache hit on a top-level function call: the function did
            // NOT execute server-side. No new state to verify.
            $this->cachedRows = $entry['rows'];
            $this->cachedColumns = $entry['columns'];
            $this->fetchIndex = 0;
            $this->fromCache = true;
            return true;
        }

        // Cache miss: execute for real
        $result = $this->realStmt->execute($params);
        if (!$result) {
            // Failed top-level function call may have partially run.
            // Mark dirty for next acquire.
            if (NativeCache::isTopLevelFunctionCall($this->sql)) {
                $this->gucState->markDirty();
            }
            return false;
        }

        // Try to cache the result. The columnCount() > 0 guard doubles as
        // a SET / RESET / LISTEN filter — those return zero columns, so
        // we never put their empty-row replies in the cache (parity with
        // the JS NON_CACHEABLE_COMMANDS skip).
        if ($this->realStmt->columnCount() > 0) {
            try {
                $rows = $this->realStmt->fetchAll(\PDO::FETCH_ASSOC);
                $columns = !empty($rows) ? array_keys($rows[0]) : [];
                $this->cache->put($this->sql, $effectiveParams, $rows, $columns, $stateHash);
                $this->cachedRows = $rows;
                $this->cachedColumns = $columns;
                $this->fetchIndex = 0;
                $this->fromCache = true;
            } catch (\Throwable $e) {
                // If caching fails, fall through to real statement
            }
        }

        $this->maybePostCallVerify($result);

        return $result;
    }

    /**
     * Trigger post-call verify after the real statement has been
     * executed, but only if this SQL is a top-level function call
     * shape AND we have a parent CachedPDO to issue the verify
     * query through. Statements built via `fromCache()` have no
     * parent — but they also never reach this branch because the
     * cache-hit return is taken before the real execute() runs.
     */
    private function maybePostCallVerify(bool $executeOk): void
    {
        if (!$executeOk) {
            return;
        }
        if ($this->parent === null) {
            return;
        }
        if (!NativeCache::isTopLevelFunctionCall($this->sql)) {
            return;
        }
        $this->parent->postCallVerify($this->sql);
    }

    public function fetch(int $mode = \PDO::FETCH_DEFAULT, ...$args): mixed
    {
        if ($this->cachedRows !== null) {
            if ($this->fetchIndex >= count($this->cachedRows)) {
                return false;
            }
            $row = $this->cachedRows[$this->fetchIndex];
            $this->fetchIndex++;

            if ($mode === \PDO::FETCH_NUM || $mode === \PDO::FETCH_BOTH) {
                $numRow = array_values($row);
                if ($mode === \PDO::FETCH_BOTH) {
                    return array_merge($numRow, $row);
                }
                return $numRow;
            }
            return $row; // FETCH_ASSOC or FETCH_DEFAULT
        }
        return $this->realStmt?->fetch($mode, ...$args) ?? false;
    }

    public function fetchAll(int $mode = \PDO::FETCH_DEFAULT, ...$args): array
    {
        if ($this->cachedRows !== null) {
            $remaining = array_slice($this->cachedRows, $this->fetchIndex);
            $this->fetchIndex = count($this->cachedRows);

            if ($mode === \PDO::FETCH_NUM) {
                return array_map('array_values', $remaining);
            }
            if ($mode === \PDO::FETCH_BOTH) {
                return array_map(function ($row) {
                    return array_merge(array_values($row), $row);
                }, $remaining);
            }
            return $remaining; // FETCH_ASSOC or FETCH_DEFAULT
        }
        return $this->realStmt?->fetchAll($mode, ...$args) ?? [];
    }

    public function fetchColumn(int $column = 0): mixed
    {
        if ($this->cachedRows !== null) {
            if ($this->fetchIndex >= count($this->cachedRows)) {
                return false;
            }
            $row = $this->cachedRows[$this->fetchIndex];
            $this->fetchIndex++;
            $values = array_values($row);
            return $values[$column] ?? false;
        }
        return $this->realStmt?->fetchColumn($column) ?? false;
    }

    public function rowCount(): int
    {
        if ($this->cachedRows !== null) {
            return count($this->cachedRows);
        }
        return $this->realStmt?->rowCount() ?? 0;
    }

    public function columnCount(): int
    {
        if ($this->cachedColumns !== null) {
            return count($this->cachedColumns);
        }
        return $this->realStmt?->columnCount() ?? 0;
    }

    public function closeCursor(): bool
    {
        $this->cachedRows = null;
        $this->cachedColumns = null;
        $this->fetchIndex = 0;
        return $this->realStmt?->closeCursor() ?? true;
    }

    public function bindValue(string|int $param, mixed $value, int $type = \PDO::PARAM_STR): bool
    {
        if ($this->params === null) {
            $this->params = [];
        }
        $this->params[$param] = $value;
        return $this->realStmt?->bindValue($param, $value, $type) ?? false;
    }

    public function bindParam(
        string|int $param,
        mixed &$var,
        int $type = \PDO::PARAM_STR,
        int $maxLength = 0,
        mixed $driverOptions = null,
    ): bool {
        if ($this->params === null) {
            $this->params = [];
        }
        $this->params[$param] = $var;
        return $this->realStmt?->bindParam($param, $var, $type, $maxLength, $driverOptions) ?? false;
    }

    public function getIterator(): \Iterator
    {
        if ($this->cachedRows !== null) {
            return new \ArrayIterator(array_slice($this->cachedRows, $this->fetchIndex));
        }
        if ($this->realStmt !== null) {
            return new \ArrayIterator($this->realStmt->fetchAll(\PDO::FETCH_ASSOC));
        }
        return new \ArrayIterator([]);
    }

    public function __call(string $method, array $args): mixed
    {
        if ($this->realStmt === null) {
            throw new \BadMethodCallException("Cannot call {$method} on a cached-only statement");
        }
        return $this->realStmt->$method(...$args);
    }
}
