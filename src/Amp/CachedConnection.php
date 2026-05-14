<?php

namespace GoldLapel\Amp;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use GoldLapel\AggressiveVerify;
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

    /**
     * Aggressive-verify mode + lazy-resolved decision. See AggressiveVerify
     * for the design; mirrors the sync CachedPDO fields exactly.
     */
    private string $aggressiveVerifyMode;
    private string $aggressiveVerifyIdentity;
    private ?bool $aggressiveVerifyActive = null;

    /**
     * Aggressive verify defaults to 'auto' across the board — the
     * bump is zero-cost (no round-trip) so there's no upside to
     * defaulting off.
     */
    public function __construct(
        private PostgresExecutor $real,
        private NativeCache $cache,
        string $aggressiveVerifyMode = AggressiveVerify::MODE_AUTO,
        ?string $aggressiveVerifyIdentity = null,
    ) {
        $this->gucState = new ConnectionGucState();
        $this->aggressiveVerifyMode = $aggressiveVerifyMode;
        $this->aggressiveVerifyIdentity = $aggressiveVerifyIdentity
            ?? 'amp:' . spl_object_hash($real);
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
     * Force the connection into the dirty state. While dirty, the L1
     * cache is bypassed for this connection — both reads and the
     * current query route to the proxy directly. Symmetrical with
     * CachedPDO::markStateDirty(); exposed for connection-reuse points
     * where a frontend hands the same wrapped CachedConnection back to
     * a new HTTP request.
     */
    public function markStateDirty(): void
    {
        $this->gucState->markDirty();
    }

    /**
     * Post-call dml_seq bump for top-level `SELECT <function>(...)`
     * shapes. Async sibling of CachedPDO::postCallVerify — the same
     * cache-key isolation strategy works without an event-loop trip
     * since the bump is pure-PHP. The "Async" suffix is retained for
     * API stability across the wrapper's earlier waves.
     */
    public function postCallVerifyAsync(string $sql): void
    {
        if (!NativeCache::isTopLevelFunctionCall($sql)) {
            return;
        }
        if (!$this->resolveAggressiveVerifyActive()) {
            return;
        }
        $this->gucState->bumpDmlSeq();
    }

    /**
     * Async sibling of CachedPDO::postWriteVerify — bumps the per-
     * connection dml_seq counter after a DML write, closing the
     * trigger-internal-SET coverage gap. Zero round-trips — just a
     * counter bump + hash recompute, no Amp\async() scheduling
     * needed. The "Async" suffix is retained for API stability.
     *
     * In Amp's fiber model the wrapper is already running in a
     * coroutine; the single-coroutine semantics + the synchronous
     * counter bump together guarantee any in-flight verify on this
     * connection holds back subsequent reads on the same coroutine
     * (PHP is single-threaded; Amp coroutines are co-operative).
     */
    public function postWriteVerifyAsync(string $sql): void
    {
        if (!$this->resolveAggressiveVerifyActive()) {
            return;
        }
        $this->gucState->bumpDmlSeq();
    }

    /**
     * Public accessor for the resolved aggressive-verify decision.
     * Mirrors CachedPDO::isAggressiveVerifyActive().
     */
    public function isAggressiveVerifyActive(): bool
    {
        return $this->resolveAggressiveVerifyActive();
    }

    /**
     * Lazy resolver — runs once per CachedConnection. Delegates to
     * AggressiveVerify::decide(); the off-mode warning fires here on
     * first call (deduped by AggressiveVerify per connection identity).
     */
    private function resolveAggressiveVerifyActive(): bool
    {
        if ($this->aggressiveVerifyActive !== null) {
            return $this->aggressiveVerifyActive;
        }
        $this->aggressiveVerifyActive = AggressiveVerify::decide(
            $this->aggressiveVerifyMode,
            $this->aggressiveVerifyIdentity,
        );
        return $this->aggressiveVerifyActive;
    }

    /**
     * Handle cache semantics for a SQL statement:
     *   - detect TX boundaries (bypass cache inside a tx)
     *   - detect writes (invalidate)
     *   - drain pending invalidation signals
     *   - bypass L1 if the connection is dirty (route straight through)
     *   - cache SELECTs when unparameterized; return cached PostgresResult
     *     via CachedResult if hit
     *   - bump dml_seq on top-level SELECT <fn>(...) for cache-key isolation
     */
    private function handle(string $sql, ?array $params, \Closure $miss): PostgresResult
    {
        $this->cache->pollSignals();

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
                // a failed SELECT that observed nothing doesn't force
                // an unnecessary L1 bypass on the next acquire.
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
            // Aggressive verify (always-on bump, opt-out via 'off'):
            // close the trigger-internal-SET coverage gap by bumping
            // dml_seq after every DML so subsequent cacheable reads on
            // this connection route to a fresh cache slot. Skipped
            // inside a tx (cache is bypassed and the bump surfaces at
            // COMMIT via the next read) and for pure DDL (we already
            // invalidate everything; the next read bypasses L1 if the
            // connection is dirty).
            if (!$this->inTransaction && $writeTables !== NativeCache::DDL_SENTINEL) {
                $this->postWriteVerifyAsync($sql);
            }
            // Post-call dml_seq bump if this write was actually a top-
            // level function call. Synchronous in the wrapper now.
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

        // Dirty-flag bypass: when the connection is marked dirty (e.g.
        // connection-reuse points, partial-failure recovery) we route
        // reads + the current execute straight through and don't seed
        // cache slots with results keyed on a state map that may not
        // match the server.
        $dirtyBypass = $this->gucState->isDirty();

        $stateHash = $this->gucState->stateHash();
        if (!$dirtyBypass) {
            $entry = $this->cache->get($sql, $params, $stateHash);
            if ($entry !== null) {
                // Cache hit on a function call: server didn't run the
                // function body, so no new state to verify.
                return new CachedResult($entry['rows']);
            }
        }

        $result = $runMiss();
        // Drain the result regardless — PostgresResult is iterable-once,
        // so consumers downstream still need a buffered replay (CachedResult).
        // But only put the slot in the cache when the result actually has a
        // column shape: SET / RESET / LISTEN / etc return PostgresCommandResult
        // whose getColumnCount() is null, and we don't want to bloat the cache
        // with empty-row entries that never serve real data. Mirrors the JS
        // NON_CACHEABLE_COMMANDS skip. Skipped when dirty.
        $rows = [];
        foreach ($result as $row) {
            $rows[] = $row;
        }
        $columns = !empty($rows) ? array_keys($rows[0]) : [];
        $colCount = $result->getColumnCount();
        if ($colCount !== null && $colCount > 0 && !$dirtyBypass) {
            $this->cache->put($sql, $params, $rows, $columns, $stateHash);
        }
        // Post-call dml_seq bump for top-level `SELECT <function>(...)`.
        // Synchronous in the wrapper now — closes the cache-key isolation
        // gap without an Amp\async() trip.
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
