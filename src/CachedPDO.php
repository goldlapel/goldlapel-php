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

    /**
     * Snapshot of unsafe-GUC state captured at BEGIN, restored on
     * ROLLBACK. PG semantics: non-LOCAL SETs issued inside an aborted
     * transaction are reverted by the server, so the wrapper-side state
     * map must follow. Null when not in a tracked transaction. Single-
     * frame — savepoints are out of scope; a `ROLLBACK TO SAVEPOINT`
     * doesn't trigger restore (NativeCache::isTxRollback rejects that
     * shape).
     *
     * @var array<string, mixed>|null
     */
    private ?array $txGucSnapshot = null;

    /**
     * Run a real PDO call with deferred-mutation discipline. The caller
     * is responsible for having snapshotted state + applied observeSql
     * BEFORE invoking the closure (so observe-derived data like
     * post-state hashes can be captured ahead of the real call). On
     * failure (`PDOException` thrown OR `false` returned), the snapshot
     * is restored and the connection is marked dirty so the next
     * acquire's verify-on-checkout pass resyncs from `pg_settings`.
     *
     * The server may apply a prefix of a multi-stmt Q before erroring
     * on a later segment; we can't tell which one failed, so restore +
     * markDirty is the safe answer — the verify pass will recover the
     * actual server state on next use.
     *
     * @template T
     * @param array<string, mixed> $snapshot  Pre-observation snapshot.
     * @param callable(): T $thunk  The real PDO call to execute.
     * @return T  Whatever the thunk returned.
     */
    private function runWithRecovery(array $snapshot, callable $thunk): mixed
    {
        try {
            $result = $thunk();
        } catch (\PDOException $e) {
            $this->revertObservation($snapshot);
            throw $e;
        }
        // PDO returns false from query()/exec() in ERRMODE_SILENT /
        // ERRMODE_WARNING when an error fires without an exception. Treat
        // exactly like the throw branch — don't trust the optimistic
        // mutation when the server reported failure.
        if ($result === false) {
            $this->revertObservation($snapshot);
        }
        return $result;
    }

    /**
     * Revert an optimistic observation by restoring the snapshot. Marks
     * dirty ONLY if the observation actually mutated state — a failed
     * SELECT that observed nothing doesn't need to trigger a verify
     * query on the next acquire (the state hash is already correct).
     *
     * @param array<string, mixed> $snapshot
     */
    private function revertObservation(array $snapshot): void
    {
        $observed = $snapshot['gucStateHash'] !== $this->gucState->stateHash();
        $this->gucState->restore($snapshot);
        if ($observed) {
            // Server may have applied a prefix of a multi-stmt body
            // before erroring on a later segment. We can't tell which,
            // so markDirty so the next acquire's verify-on-checkout
            // reconciles from pg_settings.
            $this->gucState->markDirty();
        }
    }

    /**
     * Capture the tx-boundary snapshot used to revert non-LOCAL SETs on
     * ROLLBACK. Called from beginTransaction() — captures the live
     * state map, which is correct there because beginTransaction
     * doesn't observeSql (the caller didn't pass SQL). The query() /
     * exec() paths set `$txGucSnapshot = $preSnapshot` directly instead,
     * because they observed the SQL into the live map BEFORE this could
     * fire and need the pre-observation state. Only captures on the
     * OUTER BEGIN — nested BEGIN is a server-side no-op.
     */
    private function snapshotForTxStart(): void
    {
        if ($this->txGucSnapshot === null) {
            $this->txGucSnapshot = $this->gucState->snapshot();
        }
    }

    private function dropTxSnapshot(): void
    {
        $this->txGucSnapshot = null;
    }

    private function restoreTxSnapshot(): void
    {
        if ($this->txGucSnapshot !== null) {
            $this->gucState->restore($this->txGucSnapshot);
            $this->txGucSnapshot = null;
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

        // Wave 2: capture the pre-observation snapshot so we can roll
        // the state map back if PDO reports failure. observeSql() below
        // mutates the live map optimistically; the runWithRecovery()
        // helper restores from `$preSnapshot` on PDOException / false
        // return so a SET that the server didn't apply doesn't leave
        // the wrapper diverged.
        $preSnapshot = $this->gucState->snapshot();

        // Observe unsafe-GUC SET / RESET before any other handling so the
        // state hash reflects the effective post-statement state when the
        // result is keyed (matters for `SET app.user_id = '7'; SELECT ...`
        // multi-statements). State lives on this CachedPDO so concurrent
        // connections never share gucState. The mutation is reverted on
        // failure via $preSnapshot — see runWithRecovery().
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
            // Multi-stmt bodies that contain a tx boundary AND writes
            // (`BEGIN; INSERT ...; ROLLBACK`). The tx-boundary branch
            // below isn't reached on this branch, so do snapshot /
            // drop / restore inline. Use $preSnapshot directly (not
            // snapshotForTxStart()) because for inline `BEGIN; SET ...;
            // INSERT; ROLLBACK` the SET was already observed, and we
            // want to revert to the pre-observation state.
            $finalTx = NativeCache::applyTxBoundaries($sql);
            $openedTx = self::bodyOpensTxBeforeClose($sql);
            $wasInTx = $this->inTransaction;
            if ($openedTx && !$wasInTx && $this->txGucSnapshot === null) {
                $this->txGucSnapshot = $preSnapshot;
            }
            try {
                $stmt = $this->runWithRecovery($preSnapshot, fn() => $this->pdo->query($sql, ...$args));
            } catch (\PDOException $e) {
                if ($openedTx && !$wasInTx) {
                    $this->dropTxSnapshot();
                }
                throw $e;
            }
            if ($stmt === false) {
                if ($openedTx && !$wasInTx) {
                    $this->dropTxSnapshot();
                }
                return false;
            }
            if ($finalTx === false) {
                if (NativeCache::bodyEndsWithRollback($sql)) {
                    $this->restoreTxSnapshot();
                } else {
                    $this->dropTxSnapshot();
                }
                $this->inTransaction = false;
            } elseif ($finalTx === true) {
                $this->inTransaction = true;
            }
            $this->postCallVerify($sql);
            return new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction, $this);
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
            $wasInTx = $this->inTransaction;
            // Snapshot on the open boundary, restore on a ROLLBACK end,
            // drop on a COMMIT end. Always use $preSnapshot (pre-
            // observation) — a multi-stmt body like `BEGIN; SET app.x =
            // 'y'; ...` has already had the SET applied to the live
            // map, and we need to revert to BEFORE that SET on ROLLBACK.
            if (!$wasInTx && $this->txGucSnapshot === null) {
                $opened = ($txState === true) || ($txState === false && self::bodyOpensTxBeforeClose($sql));
                if ($opened) {
                    $this->txGucSnapshot = $preSnapshot;
                }
            }
            try {
                $stmt = $this->runWithRecovery($preSnapshot, fn() => $this->pdo->query($sql, ...$args));
            } catch (\PDOException $e) {
                // Server didn't actually transition tx state — roll
                // wrapper-side flag back, drop the snapshot we just
                // captured. runWithRecovery already reverted gucState.
                if (!$wasInTx) {
                    $this->dropTxSnapshot();
                    $this->inTransaction = $wasInTx;
                }
                throw $e;
            }
            if ($stmt === false) {
                // Server rejected the boundary; same recovery as the
                // throw arm.
                if (!$wasInTx) {
                    $this->dropTxSnapshot();
                    $this->inTransaction = $wasInTx;
                }
                return false;
            }
            $this->inTransaction = $txState;
            if ($txState === false) {
                if (NativeCache::bodyEndsWithRollback($sql)) {
                    $this->restoreTxSnapshot();
                } else {
                    $this->dropTxSnapshot();
                }
            }
            return new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction, $this);
        }

        // Inside transaction: bypass cache. Post-call verify is also
        // skipped — server-side state mutations made inside an open
        // transaction won't affect another connection's cache reads
        // until the tx commits, and the wrapper's own state hash
        // doesn't matter while inTransaction=true (cache is bypassed
        // anyway). Verify will be triggered as needed once the tx ends
        // and the next read runs.
        if ($this->inTransaction) {
            $stmt = $this->runWithRecovery($preSnapshot, fn() => $this->pdo->query($sql, ...$args));
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
            $stmt = $this->runWithRecovery($preSnapshot, fn() => $this->pdo->query($sql, ...$args));
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction, $this) : false;
        }

        // Read path: check native cache, keyed on this connection's
        // (post-observation) GUC fingerprint.
        $stateHash = $this->gucState->stateHash();
        $entry = $this->cache->get($sql, null, $stateHash);
        if ($entry !== null) {
            // Cache hit: function body did NOT execute server-side, so
            // there's no new state to verify. Return without verify.
            return CachedPDOStatement::fromCache($entry, $this->cache, $this->gucState, $sql);
        }

        // Cache miss: execute for real with deferred-mutation discipline.
        try {
            $stmt = $this->runWithRecovery($preSnapshot, fn() => $this->pdo->query($sql, ...$args));
        } catch (\PDOException $e) {
            // Even on failure, a top-level function call may have
            // partially executed before erroring out. runWithRecovery
            // already restored + marked dirty; markDirty is idempotent
            // so the explicit re-mark is harmless and documents intent.
            if (NativeCache::isTopLevelFunctionCall($sql)) {
                $this->gucState->markDirty();
            }
            throw $e;
        }
        if ($stmt === false) {
            // runWithRecovery already restored + marked dirty. The
            // function-call branch is now redundant but kept for
            // documentation parity with the throw arm.
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

    /**
     * True if a multi-statement body contains a BEGIN segment somewhere
     * before its closing tx-boundary segment. Approximates "a transaction
     * was opened during the body" — used for the BEGIN; ROLLBACK case
     * inside the write-detect branch.
     */
    private static function bodyOpensTxBeforeClose(string $sql): bool
    {
        if ($sql === '') {
            return false;
        }
        $tail = rtrim($sql);
        if (str_ends_with($tail, ';')) {
            $tail = rtrim(substr($tail, 0, -1));
        }
        if (!str_contains($tail, ';')) {
            return NativeCache::isTxStart($sql);
        }
        foreach (NativeCache::splitStatements($sql) as $seg) {
            if (NativeCache::isTxStart($seg)) {
                return true;
            }
        }
        return false;
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

        // Wave 2 deferred-mutation discipline (mirrors query()): snapshot
        // BEFORE observing so we can revert if PDO reports failure.
        $preSnapshot = $this->gucState->snapshot();

        // Observe unsafe-GUC SET / RESET before write detection. Mirrors
        // query(); a `SET app.user_id = '7'` issued via exec() must
        // update this connection's state hash so subsequent reads on the
        // same CachedPDO key correctly. Reverted on failure via
        // $preSnapshot.
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
        $wasInTx = $this->inTransaction;
        if ($txState !== null) {
            $this->inTransaction = $txState;
        }
        // Snapshot the pre-tx state for ROLLBACK revert. Always use
        // $preSnapshot (the pre-observation state), not the live map —
        // a multi-statement Q like `BEGIN; SET app.x = 'y'; ...` has
        // already had the SET applied to the live map by observeSql,
        // and we want to revert to BEFORE that SET on ROLLBACK.
        if (!$wasInTx && $this->txGucSnapshot === null) {
            $opened = ($txState === true) || ($txState === false && self::bodyOpensTxBeforeClose($sql));
            if ($opened) {
                $this->txGucSnapshot = $preSnapshot;
            }
        }
        try {
            $result = $this->runWithRecovery($preSnapshot, fn() => $this->pdo->exec($sql));
        } catch (\PDOException $e) {
            // If we'd just opened a tx (or opened-and-closed one in a
            // single batch), drop the snapshot — the BEGIN didn't
            // survive the failed batch (server aborted), and
            // runWithRecovery already restored the GUC map from
            // $preSnapshot. Roll the tx flag back to its pre-call value.
            if (!$wasInTx && $this->txGucSnapshot !== null) {
                $this->dropTxSnapshot();
                $this->inTransaction = $wasInTx;
            }
            throw $e;
        }
        // Same recovery on the false-return path.
        if ($result === false && !$wasInTx && $this->txGucSnapshot !== null && $txState !== null) {
            $this->dropTxSnapshot();
            $this->inTransaction = $wasInTx;
        }
        // Post-call verify if this exec was a top-level function call
        // (`SELECT my_handler()` issued via exec). Skip inside a tx —
        // see CachedPDO::query() for the rationale.
        if (!$this->inTransaction && $result !== false) {
            $this->postCallVerify($sql);
        } elseif ($result === false && NativeCache::isTopLevelFunctionCall($sql)) {
            $this->gucState->markDirty();
        }
        // Resolve tx-snapshot lifecycle for end-of-tx bodies.
        // applyTxBoundaries returns false iff the body's last boundary
        // is COMMIT or ROLLBACK; bodyEndsWithRollback distinguishes them.
        if ($txState === false) {
            if (NativeCache::bodyEndsWithRollback($sql)) {
                $this->restoreTxSnapshot();
            } else {
                $this->dropTxSnapshot();
            }
        }
        return $result;
    }

    public function beginTransaction(): bool
    {
        // Snapshot BEFORE the server-side BEGIN actually fires so a
        // ROLLBACK can revert any non-LOCAL `SET app.x` issued during
        // the tx. Only the outer BEGIN takes a snapshot; nested BEGINs
        // are server-side warnings (no-op).
        $wasInTx = $this->inTransaction;
        $this->inTransaction = true;
        if (!$wasInTx) {
            $this->snapshotForTxStart();
        }
        try {
            $ok = $this->pdo->beginTransaction();
        } catch (\Throwable $e) {
            // Server-side BEGIN failed — drop the snapshot we just took
            // and roll the tx flag back. State map is unchanged so
            // there's nothing to restore.
            if (!$wasInTx) {
                $this->dropTxSnapshot();
                $this->inTransaction = false;
            }
            throw $e;
        }
        if (!$ok && !$wasInTx) {
            // Same recovery for the false-return path.
            $this->dropTxSnapshot();
            $this->inTransaction = false;
        }
        return $ok;
    }

    public function commit(): bool
    {
        $this->inTransaction = false;
        // COMMIT keeps any non-LOCAL SETs issued during the tx, so we
        // simply drop the snapshot.
        $this->dropTxSnapshot();
        return $this->pdo->commit();
    }

    public function rollBack(): bool
    {
        $this->inTransaction = false;
        // ROLLBACK reverts non-LOCAL SETs issued during the tx server-
        // side, so the wrapper-side state must be reverted to the
        // pre-BEGIN snapshot to stay in sync. Restoration runs BEFORE
        // the real call so a `rollBack()` failure still leaves a
        // consistent wrapper state — the connection is in an indeterminate
        // server state at that point anyway, and the next acquire will
        // hit verify-on-checkout if any path marked dirty during the tx
        // (e.g. a stored-function call that ran inside the tx).
        $this->restoreTxSnapshot();
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

        // Wave 2 deferred-mutation discipline (mirrors CachedPDO::query):
        // snapshot BEFORE observe so we can roll the state map back if
        // the prepared statement's execute() reports failure.
        $preSnapshot = $this->gucState->snapshot();

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
            $r = $this->runRealExecuteWithRecovery($params, $preSnapshot);
            $this->maybePostCallVerify($r);
            return $r;
        }

        // Transaction tracking
        if (NativeCache::isTxStart($this->sql)) {
            return $this->runRealExecuteWithRecovery($params, $preSnapshot);
        }
        if (NativeCache::isTxEnd($this->sql)) {
            return $this->runRealExecuteWithRecovery($params, $preSnapshot);
        }

        // Inside transaction: bypass cache. Post-call verify is also
        // skipped — see CachedPDO::query() for the rationale.
        if (($this->inTransaction)()) {
            return $this->runRealExecuteWithRecovery($params, $preSnapshot);
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

        // Cache miss: execute for real with deferred-mutation discipline.
        $result = $this->runRealExecuteWithRecovery($params, $preSnapshot);
        if (!$result) {
            // Failed top-level function call may have partially run.
            // Mark dirty for next acquire (markDirty already called
            // inside runRealExecuteWithRecovery on a false return).
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
     * Wave 2 deferred-mutation helper for the prepare/execute path.
     * Mirrors CachedPDO::runWithRecovery() but is local to the
     * statement so we don't need a back-reference.
     *
     * @param ?array<int|string, mixed> $params
     * @param array<string, mixed> $snapshot
     */
    private function runRealExecuteWithRecovery(?array $params, array $snapshot): bool
    {
        try {
            $r = $this->realStmt->execute($params);
        } catch (\PDOException $e) {
            $this->revertObservation($snapshot);
            throw $e;
        }
        if ($r === false) {
            $this->revertObservation($snapshot);
        }
        return $r;
    }

    /**
     * Mirror of CachedPDO::revertObservation — restore + conditionally
     * markDirty (only if observeSql actually mutated state).
     *
     * @param array<string, mixed> $snapshot
     */
    private function revertObservation(array $snapshot): void
    {
        $observed = $snapshot['gucStateHash'] !== $this->gucState->stateHash();
        $this->gucState->restore($snapshot);
        if ($observed) {
            $this->gucState->markDirty();
        }
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
