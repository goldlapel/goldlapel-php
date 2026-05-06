<?php

namespace GoldLapel\Amp;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use GoldLapel\ConnectionGucState;
use GoldLapel\NativeCache;

/**
 * Async L1-cached wrapper around a PostgresExecutor (connection or
 * transaction).
 *
 * Mirrors the sync CachedPDO, with two important differences:
 *
 * 1. The cache layer is purely synchronous — NativeCache is an in-process
 *    hash-table with no I/O. It's safe to read/write from any fiber.
 * 2. Invalidation signals arrive on a Unix/TCP socket the Rust proxy
 *    broadcasts on. Rather than poll in a dedicated fiber, we drain
 *    pending signals opportunistically on every query (same model as
 *    sync CachedPDO) — this keeps dirty data from being served on reads
 *    while staying lock-free.
 *
 * Only the high-traffic read path (SELECT without TX) hits the cache.
 * Writes invalidate the affected tables; DDL invalidates all.
 *
 * This wrapper implements PostgresExecutor so it can be passed anywhere
 * a real executor is accepted — including GoldLapel\Amp\Utils methods
 * and into using() scopes.
 */
class CachedConnection implements PostgresExecutor
{
    use \Amp\ForbidCloning;
    use \Amp\ForbidSerialization;

    private bool $inTransaction = false;

    /**
     * Per-connection GUC state — see ConnectionGucState. Each
     * CachedConnection has its own, never shared with a peer fiber's
     * connection, so a `SET app.user_id` on connection A can't poison
     * cache reads on connection B.
     */
    private ConnectionGucState $gucState;

    /**
     * Snapshot of unsafe-GUC state captured at BEGIN, restored on
     * ROLLBACK. Mirrors CachedPDO::$txGucSnapshot — see that field for
     * the PG-tx-scoped-revert rationale.
     *
     * @var array<string, mixed>|null
     */
    private ?array $txGucSnapshot = null;

    public function __construct(
        private PostgresExecutor $real,
        private NativeCache $cache,
    ) {
        $this->gucState = new ConnectionGucState();
    }

    public function getWrappedExecutor(): PostgresExecutor
    {
        return $this->real;
    }

    public function unwrap(): PostgresExecutor
    {
        return $this->real;
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
     * marked dirty. Async sibling of CachedPDO::verifyIfDirty() — runs
     * inline on the current fiber; the caller is already in a fiber so
     * the verify just yields a step in the event loop while the proxy
     * round-trips. Idempotent and silent on failure (next acquire
     * retries).
     */
    public function verifyIfDirty(): bool
    {
        if (!$this->gucState->isDirty()) {
            return false;
        }
        try {
            $result = $this->real->query(
                "SELECT name, setting FROM pg_settings WHERE source='session'"
            );
            $rows = [];
            foreach ($result as $row) {
                $rows[] = $row;
            }
            $this->gucState->applyVerifyResult($rows);
            return true;
        } catch (\Throwable $e) {
            // Connection stays dirty; next acquire retries. Never
            // bubbles up — the user's query hasn't started yet.
            return false;
        }
    }

    /**
     * Force the connection into the dirty state. Symmetrical with
     * CachedPDO::markStateDirty() — exposed for `Amp\CachedConnection`
     * reuse points where a frontend hands the same wrapped connection
     * back to a new HTTP request.
     */
    public function markStateDirty(): void
    {
        $this->gucState->markDirty();
    }

    /**
     * Schedule a post-call verify in a parallel coroutine via
     * `Amp\async()`. The user's query response is already captured
     * before this fires, so the verify runs without blocking the
     * caller. On failure we mark the connection dirty so the next
     * acquire path retries — never propagated as a user-visible error.
     *
     * Public so the executor's call paths AND any future statement
     * wrapper can both invoke it.
     */
    public function postCallVerifyAsync(string $sql): void
    {
        if (!NativeCache::isTopLevelFunctionCall($sql)) {
            return;
        }
        // Capture the connection + state by reference — the inner
        // closure runs on a sibling coroutine that may outlive the
        // calling fiber's frame. ForbidCloning on this class is
        // enforced by the trait, so the closure can't accidentally
        // resurrect a duplicate.
        $real = $this->real;
        $state = $this->gucState;
        \Amp\async(static function () use ($real, $state, $sql) {
            try {
                $result = $real->query(
                    "SELECT name, setting FROM pg_settings WHERE source='session'"
                );
                $rows = [];
                foreach ($result as $row) {
                    $rows[] = $row;
                }
                $state->applyVerifyResult($rows);
            } catch (\Throwable $e) {
                $state->markDirty();
            }
        });
    }

    /**
     * Handle cache semantics for a SQL statement:
     *   - run verify-on-checkout if dirty
     *   - detect TX boundaries (bypass cache inside a tx)
     *   - detect writes (invalidate)
     *   - drain pending invalidation signals
     *   - cache SELECTs when unparameterized; return cached PostgresResult
     *     via CachedResult if hit
     *   - schedule async post-call verify on top-level SELECT <fn>(...)
     */
    private function handle(string $sql, ?array $params, \Closure $miss): PostgresResult
    {
        $this->cache->pollSignals();

        // Verify-on-checkout fallback. No-op on clean connections.
        $this->verifyIfDirty();

        // Wave 2 deferred-mutation discipline: snapshot BEFORE observe so
        // a thrown exception from the real `$miss()` call rolls the
        // wrapper-side state map back. Mirrors CachedPDO::$preSnapshot.
        $preSnapshot = $this->gucState->snapshot();

        // Observe unsafe-GUC SET / RESET so this connection's state hash
        // reflects the post-statement state when the result is keyed
        // (mirrors the sync CachedPDO path). State is per-connection —
        // shared use of a single NativeCache across many fibers is
        // expected, but each CachedConnection has its own gucState.
        $this->gucState->observeSql($sql);

        $runMiss = function () use ($miss, $preSnapshot) {
            try {
                return $miss();
            } catch (\Throwable $e) {
                // Server didn't apply the SET (or aborted partway through
                // a multi-stmt body). Revert the optimistic mutation;
                // mark dirty IF observation actually mutated state, so
                // a failed SELECT that observed nothing doesn't trigger
                // an unnecessary verify query on the next acquire.
                // amphp/postgres throws SqlQueryError / SqlException —
                // the catch-all is appropriate since any exception means
                // the result is unusable anyway.
                $observed = $preSnapshot['gucStateHash'] !== $this->gucState->stateHash();
                $this->gucState->restore($preSnapshot);
                if ($observed) {
                    $this->gucState->markDirty();
                }
                throw $e;
            }
        };

        // Multi-statement-aware write detection. Runs BEFORE transaction
        // tracking so a single Q message like `BEGIN; INSERT INTO orders
        // VALUES (1); COMMIT` still surfaces the INSERT for invalidation
        // — TX_START's first-token check would otherwise swallow the
        // whole body and the INSERT would never invalidate the orders
        // cache slot. Same gap fixed for `SET app.tenant = 'x'; INSERT
        // INTO t ...` (SET hides the INSERT from detectWrite's
        // single-token shape).
        $writeTables = NativeCache::detectWritesMulti($sql);
        if ($writeTables !== null) {
            if ($writeTables === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                foreach ($writeTables as $t) {
                    $this->cache->invalidateTable($t);
                }
            }
            $finalTx = NativeCache::applyTxBoundaries($sql);
            $opensTx = self::bodyOpensTxBeforeClose($sql);
            $wasInTx = $this->inTransaction;
            $capturedTx = false;
            if ($opensTx && !$wasInTx && $this->txGucSnapshot === null) {
                $this->txGucSnapshot = $preSnapshot;
                $capturedTx = true;
            }
            try {
                $r = $runMiss();
            } catch (\Throwable $e) {
                if ($capturedTx) {
                    $this->txGucSnapshot = null;
                }
                throw $e;
            }
            if ($finalTx === false) {
                $this->settleTxBoundary($sql);
                $this->inTransaction = false;
            } elseif ($finalTx === true) {
                $this->inTransaction = true;
            }
            // Schedule async post-call verify if this write was actually
            // a top-level function call. Returns immediately; verify runs
            // in a sibling coroutine.
            $this->postCallVerifyAsync($sql);
            return $r;
        }

        // Transaction tracking. Walks every segment in multi-statement
        // bodies so a Q like `BEGIN; LISTEN foo; COMMIT` ends with
        // inTransaction=false — matching the server. Pre-fix, only the
        // leading BEGIN was seen and the wrapper stayed stuck thinking a
        // tx was open, bypassing the cache for every subsequent read
        // until a fresh BEGIN/COMMIT cycle reset state. Single-statement
        // bodies skip the splitter.
        $txState = NativeCache::applyTxBoundaries($sql);
        if ($txState !== null) {
            $wasInTx = $this->inTransaction;
            $capturedTx = false;
            if (!$wasInTx && $this->txGucSnapshot === null) {
                $opened = ($txState === true) || ($txState === false && self::bodyOpensTxBeforeClose($sql));
                if ($opened) {
                    $this->txGucSnapshot = $preSnapshot;
                    $capturedTx = true;
                }
            }
            try {
                $r = $runMiss();
            } catch (\Throwable $e) {
                if ($capturedTx) {
                    $this->txGucSnapshot = null;
                }
                throw $e;
            }
            $this->inTransaction = $txState;
            if ($txState === false) {
                $this->settleTxBoundary($sql);
            }
            return $r;
        }

        // Inside a tx: bypass cache + skip post-call verify (cache is
        // bypassed anyway and verify won't see GUC changes until commit).
        if ($this->inTransaction) {
            return $runMiss();
        }

        $stateHash = $this->gucState->stateHash();
        $entry = $this->cache->get($sql, $params, $stateHash);
        if ($entry !== null) {
            // Cache hit on a function call: server didn't run the
            // function body, so no new state to verify.
            return new CachedResult($entry['rows']);
        }

        $result = $runMiss();
        // Drain the result regardless — PostgresResult is iterable-once,
        // so consumers downstream still need a buffered replay (CachedResult).
        // But only put the slot in the cache when the result actually has a
        // column shape: SET / RESET / LISTEN / etc return PostgresCommandResult
        // whose getColumnCount() is null, and we don't want to bloat the cache
        // with empty-row entries that never serve real data. Mirrors the JS
        // NON_CACHEABLE_COMMANDS skip.
        $rows = [];
        foreach ($result as $row) {
            $rows[] = $row;
        }
        $columns = !empty($rows) ? array_keys($rows[0]) : [];
        $colCount = $result->getColumnCount();
        if ($colCount !== null && $colCount > 0) {
            $this->cache->put($sql, $params, $rows, $columns, $stateHash);
        }
        // Async post-call verify. Returns immediately; the verify runs
        // in a sibling coroutine and updates state when complete. The
        // caller's response (via CachedResult) is already captured.
        $this->postCallVerifyAsync($sql);
        return new CachedResult($rows);
    }

    /**
     * Resolve the tx-snapshot lifecycle at a closing tx boundary.
     * COMMIT drops the snapshot; ROLLBACK restores it (reverts non-LOCAL
     * SETs issued during the tx). Mirrors CachedPDO's lifecycle helpers.
     */
    private function settleTxBoundary(string $sql): void
    {
        if (NativeCache::bodyEndsWithRollback($sql)) {
            if ($this->txGucSnapshot !== null) {
                $this->gucState->restore($this->txGucSnapshot);
                $this->txGucSnapshot = null;
            }
            // Note: we don't markDirty() here. The post-restore state
            // matches the server (both reverted to pre-BEGIN), and any
            // stored-function side effects inside the tx were also
            // reverted by PG. If a stored function called during the tx
            // marked dirty via its own path, that flag is already set.
        } else {
            $this->txGucSnapshot = null;
        }
    }

    /**
     * Mirror of CachedPDO::bodyOpensTxBeforeClose — true if the body
     * contains a BEGIN segment somewhere before its closing tx boundary.
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

    // ---- PostgresExecutor / SqlExecutor interface ----

    public function query(string $sql): PostgresResult
    {
        return $this->handle($sql, null, fn() => $this->real->query($sql));
    }

    public function execute(string $sql, array $params = []): PostgresResult
    {
        return $this->handle($sql, $params, fn() => $this->real->execute($sql, $params));
    }

    public function prepare(string $sql): \Amp\Postgres\PostgresStatement
    {
        // Prepared statements bypass the L1 read cache: they're usually
        // parameterized reads we'd cache per-param anyway. Keep simple —
        // just forward. If demand surfaces, wrap PostgresStatement too.
        return $this->real->prepare($sql);
    }

    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        return $this->real->notify($channel, $payload);
    }

    public function quoteLiteral(string $data): string
    {
        return $this->real->quoteLiteral($data);
    }

    public function quoteIdentifier(string $name): string
    {
        return $this->real->quoteIdentifier($name);
    }

    public function escapeByteA(string $data): string
    {
        return $this->real->escapeByteA($data);
    }

    public function isClosed(): bool
    {
        return $this->real->isClosed();
    }

    public function close(): void
    {
        $this->real->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->real->onClose($onClose);
    }

    public function getLastUsedAt(): int
    {
        return $this->real->getLastUsedAt();
    }
}
