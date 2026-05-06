<?php

namespace GoldLapel;

/**
 * Per-connection unsafe-GUC state tracker for L1 cache-key safety
 * (Option Y — mirrors `goldlapel/src/guc_state.rs` and the per-connection
 * trackers in goldlapel-python / goldlapel-ruby / goldlapel-js).
 *
 * Custom-GUC-driven RLS (`SET app.user_id = '42'; SELECT * FROM accounts;`
 * where the policy reads `current_setting('app.user_id')`) is a real
 * cache leak: keying purely by SQL+params, user A's cached rows could
 * be served to user B. This tracker fingerprints the unsafe-GUC subset
 * for ONE connection and folds the fingerprint into the cache key.
 *
 * State MUST be per-connection — a process can have many CachedPDO /
 * CachedConnection instances pointing at the same singleton NativeCache.
 * Putting state on the cache itself would let connection A's `SET
 * app.user_id = 'A'` poison connection B's reads of the same SQL,
 * defeating the entire purpose of the state hash.
 *
 * `SET LOCAL` is observed-but-ignored: the cache is gated on
 * transaction-idle, so SET LOCAL effects never influence a cacheable
 * response. Tracking the parser shape lets tests assert on it without
 * affecting state.
 *
 * The dirty-flag + verify-on-checkout machinery exists for cases the
 * wire-side parser can't reach: stored-function SETs, persistent-PDO
 * reuse across requests, and any post-call verify we kicked off
 * asynchronously (Amp). On reuse, callers query
 * `SELECT name, setting FROM pg_settings WHERE source='session'`
 * and feed the result into `applyVerifyResult()`, which rebuilds the
 * unsafe-subset state map. See `ConnectionGucState::isDirty()` /
 * `markDirty()` / `clearDirty()`.
 */
class ConnectionGucState
{
    /**
     * Unsafe-GUC state map: lowercased name → raw value. Only unsafe GUCs
     * (per UNSAFE_GUC_SHORT_LIST + namespace `.` rule from NativeCache)
     * ever enter the map; harmless GUCs (timezone, application_name,
     * work_mem, etc.) are never tracked and never affect the hash.
     *
     * @var array<string, string>
     */
    private array $gucState = [];

    /**
     * Cached hex hash of $gucState, recomputed on every mutation. The
     * string `"0"` is used for the empty (default) state — matches the
     * proxy's u64 zero formatted as `{:x}`. Including this in the cache
     * key means a fresh connection's slot is shared with peer connections
     * that also haven't set any unsafe GUCs (the `SET app.user_id`-free
     * majority), which is exactly what we want.
     */
    private string $gucStateHash = '0';

    /**
     * Connection has had a state mutation since the last verify-on-
     * checkout pass cleared the flag. The wrapper sets this defensively
     * after any path that *might* have mutated GUCs we can't see — most
     * notably top-level stored-function calls (`SELECT my_handler()`),
     * post-call verify failures, and persistent-connection reuse points.
     *
     * Pure SET-on-the-wire mutations DON'T mark dirty — those are
     * observable and the state map is already authoritative. Dirty means
     * "we may be out of sync with the server; verify before using cache
     * keys derived from this state".
     */
    private bool $dirty = false;

    /**
     * Tracks whether a DISCARD ALL has been observed since the dirty
     * flag was last set. After DISCARD ALL the server-side state is
     * known to be empty (we've cleared our map to match), so the
     * verify-on-checkout pass is unnecessary regardless of $dirty.
     * This stays true until the next markDirty() resets it.
     */
    private bool $discardObservedSinceDirty = false;

    /**
     * Read-only accessor for the current state hash. Hex-encoded
     * lowercase 64-bit value, or `"0"` for empty state.
     */
    public function stateHash(): string
    {
        return $this->gucStateHash;
    }

    /**
     * True if the connection's state may be out of sync with the server
     * — i.e. a verify-on-checkout pass should run before relying on the
     * state hash for cache keying. False after construction, after a
     * successful applyVerifyResult(), or once a DISCARD ALL has been
     * observed since the most recent markDirty().
     */
    public function isDirty(): bool
    {
        // DISCARD ALL is the universal "state is empty" signal, so a
        // post-discard connection is implicitly clean even if it was
        // dirty before — no verify needed.
        if ($this->discardObservedSinceDirty) {
            return false;
        }
        return $this->dirty;
    }

    /**
     * Mark this connection as potentially out-of-sync with the server.
     * Called by the wrapper after stored-function calls and on persistent-
     * connection reuse points. Idempotent.
     */
    public function markDirty(): void
    {
        $this->dirty = true;
        // A fresh markDirty() invalidates any prior DISCARD-clean state
        // because something happened after the DISCARD that may have
        // re-mutated GUCs.
        $this->discardObservedSinceDirty = false;
    }

    /**
     * Clear the dirty flag without otherwise touching state. Used by
     * the verify-on-checkout path after applyVerifyResult() succeeds —
     * the state map is now authoritative again.
     */
    public function clearDirty(): void
    {
        $this->dirty = false;
        $this->discardObservedSinceDirty = false;
    }

    /**
     * Capture an opaque snapshot of the full tracker state — unsafe-GUC
     * map + cached hash + dirty flag + DISCARD-since-dirty flag. Used by
     * the SET-actually-applied machinery to defer mutation until PDO
     * reports success: snapshot before observe, restore on PDOException.
     * Also reused by the BEGIN/ROLLBACK path so a non-LOCAL `SET app.x`
     * issued inside an aborted transaction is reverted on the wrapper
     * side, matching the server's transaction-scoped revert.
     *
     * The returned shape is intentionally opaque — callers MUST pass it
     * back to `restore()` unmodified. We return a plain associative array
     * (rather than a typed value object) to keep the snapshot allocation
     * cheap; PHP's copy-on-write on arrays makes the per-call overhead
     * negligible for the common SET-free path.
     *
     * @return array{
     *   gucState: array<string, string>,
     *   gucStateHash: string,
     *   dirty: bool,
     *   discardObservedSinceDirty: bool,
     * }
     */
    public function snapshot(): array
    {
        return [
            'gucState' => $this->gucState,
            'gucStateHash' => $this->gucStateHash,
            'dirty' => $this->dirty,
            'discardObservedSinceDirty' => $this->discardObservedSinceDirty,
        ];
    }

    /**
     * Restore a previously captured snapshot. Counterpart of `snapshot()`.
     * Used by:
     *   - Wave 2 SET-actually-applied: revert observed mutations when
     *     PDO reports failure (server didn't apply the SET).
     *   - BEGIN/ROLLBACK: revert session GUCs set inside an aborted
     *     transaction to match PG's tx-scoped revert of non-LOCAL SETs.
     *
     * @param array{
     *   gucState: array<string, string>,
     *   gucStateHash: string,
     *   dirty: bool,
     *   discardObservedSinceDirty: bool,
     * } $snapshot
     */
    public function restore(array $snapshot): void
    {
        $this->gucState = $snapshot['gucState'];
        $this->gucStateHash = $snapshot['gucStateHash'];
        $this->dirty = $snapshot['dirty'];
        $this->discardObservedSinceDirty = $snapshot['discardObservedSinceDirty'];
    }

    /**
     * Replace the unsafe-GUC state map with the result of a server-side
     * verify query. Caller is responsible for issuing
     *   SELECT name, setting FROM pg_settings WHERE source='session'
     * and passing the rows in. We filter to the unsafe subset (via
     * NativeCache::isUnsafeGuc) so the state hash exactly matches what
     * a wire-side observer would have computed.
     *
     * Accepts either:
     *   - list<array{name: string, setting: string}>     (PDO FETCH_ASSOC)
     *   - list<list{0: string, 1: string}>               (FETCH_NUM)
     *   - list<array<string, string>> with arbitrary key casing —
     *     name / setting are looked up case-insensitively.
     *
     * @param list<array<int|string, mixed>> $rows
     */
    public function applyVerifyResult(array $rows): void
    {
        $next = [];
        foreach ($rows as $row) {
            [$name, $value] = self::extractNameValue($row);
            if ($name === null || $value === null) {
                continue;
            }
            $lower = strtolower($name);
            if (!NativeCache::isUnsafeGuc($lower)) {
                continue;
            }
            $next[$lower] = (string) $value;
        }
        $this->gucState = $next;
        $this->recomputeStateHash();
        $this->clearDirty();
    }

    /**
     * Pull (name, setting) out of a single pg_settings row regardless of
     * fetch mode. Returns [null, null] for rows that don't carry both
     * fields — those are silently skipped.
     *
     * @param array<int|string, mixed> $row
     * @return array{0: ?string, 1: ?string}
     */
    private static function extractNameValue(array $row): array
    {
        // Positional (FETCH_NUM) — first two columns are name, setting in
        // the canonical SELECT order.
        if (array_key_exists(0, $row) && array_key_exists(1, $row)) {
            $name = $row[0];
            $value = $row[1];
            if (is_string($name) && (is_string($value) || is_numeric($value))) {
                return [$name, (string) $value];
            }
        }
        // Associative (FETCH_ASSOC) — case-insensitive key lookup, since
        // some drivers (PgSQL with a PDO wrapper that quotes identifiers)
        // can return capitalised keys.
        $name = self::lookupCi($row, 'name');
        $value = self::lookupCi($row, 'setting');
        if ($name === null) {
            // pg_settings's column is technically `name`, but a few
            // alternate patterns we accept defensively.
            $name = self::lookupCi($row, 'guc')
                ?? self::lookupCi($row, 'parameter');
        }
        if ($value === null) {
            $value = self::lookupCi($row, 'value');
        }
        if (is_string($name) && (is_string($value) || is_numeric($value))) {
            return [$name, (string) $value];
        }
        return [null, null];
    }

    /**
     * @param array<int|string, mixed> $row
     */
    private static function lookupCi(array $row, string $needle): mixed
    {
        $needleLower = strtolower($needle);
        foreach ($row as $k => $v) {
            if (is_string($k) && strtolower($k) === $needleLower) {
                return $v;
            }
        }
        return null;
    }

    /**
     * Observe a SQL string and apply any SET / RESET / DISCARD / set_config
     * commands it contains to this connection's unsafe-GUC state.
     * Multi-statement bodies are split on top-level `;` (string-literal-
     * aware) so a single Q like `SET app.user_id = '42'; SELECT 1` still
     * updates state.
     *
     * Returns true if the call mutated the state hash, false otherwise —
     * convenient for tests; the wrapper hot path doesn't need the return
     * value.
     */
    public function observeSql(string $sql): bool
    {
        $before = $this->gucStateHash;
        // Fast path for the common single-statement case — avoid the
        // splitter alloc when there's no inner `;`. The fast path uses a
        // naive `str_contains(';')` that won't see `;` inside quoted
        // strings; that's a perf wart, not a correctness bug — falling
        // through to the slow path still produces the right answer.
        $tail = rtrim($sql);
        if (str_ends_with($tail, ';')) {
            $tail = rtrim(substr($tail, 0, -1));
        }
        if (!str_contains($tail, ';')) {
            $cmd = NativeCache::parseSetCommand($sql);
            if ($cmd !== null) {
                $this->applySetCommand($cmd);
            }
        } else {
            foreach (NativeCache::splitStatements($sql) as $stmt) {
                $cmd = NativeCache::parseSetCommand($stmt);
                if ($cmd !== null) {
                    $this->applySetCommand($cmd);
                }
            }
        }
        return $this->gucStateHash !== $before;
    }

    /**
     * Apply a parsed SET / RESET / DISCARD / set_config command to this
     * connection's unsafe-GUC state. No-op for `set_local` /
     * `set_config_local` (transient — cache is gated on
     * transaction-idle), no-op for safe GUC names. Recomputes the cached
     * state hash on mutation.
     *
     * @param array{type: string, name: ?string, value: ?string} $cmd
     */
    private function applySetCommand(array $cmd): void
    {
        switch ($cmd['type']) {
            case 'set':
            case 'set_config':
                if ($cmd['name'] !== null && NativeCache::isUnsafeGuc($cmd['name'])) {
                    $this->gucState[$cmd['name']] = (string) $cmd['value'];
                    $this->recomputeStateHash();
                }
                break;
            case 'set_local':
            case 'set_config_local':
                // Intentionally ignored. SET LOCAL only takes effect inside
                // a transaction; the proxy's cache is gated on
                // transaction-idle, so SET LOCAL never influences a
                // cacheable response.
                break;
            case 'reset':
                if ($cmd['name'] !== null
                    && NativeCache::isUnsafeGuc($cmd['name'])
                    && array_key_exists($cmd['name'], $this->gucState)
                ) {
                    unset($this->gucState[$cmd['name']]);
                    $this->recomputeStateHash();
                }
                break;
            case 'reset_all':
            case 'discard_all':
                // DISCARD ALL is equivalent to RESET ALL plus a few other
                // session-state resets we don't track (sequences / temp
                // tables / prepared plans). For the unsafe-GUC subset, the
                // semantics are identical: clear the whole map.
                if (!empty($this->gucState)) {
                    $this->gucState = [];
                    $this->recomputeStateHash();
                }
                // DISCARD ALL is also the "server is now in default
                // state" signal — even if we were dirty before, we're
                // back in sync as of now. Set the flag unconditionally
                // (independent of whether $gucState was already empty)
                // so isDirty() returns false post-DISCARD even when our
                // wrapper-side map was already empty but the connection
                // had been marked dirty by an earlier code path.
                if ($cmd['type'] === 'discard_all') {
                    $this->discardObservedSinceDirty = true;
                }
                break;
            case 'discard_plans':
                // Plan flush — affects prepared statement caches, not
                // GUCs. We don't maintain a wrapper-side prepared-plan
                // cache (PDO::prepare's plan cache is server-side), so
                // this is a state no-op. Tracked as a recognised shape
                // for parser symmetry / parity with the proxy.
                break;
            case 'discard_noop':
                // DISCARD SEQUENCES / TEMP / TEMPORARY — no effect on
                // session-level GUCs.
                break;
        }
    }

    /**
     * Recompute the hash from $gucState. Sorted-key serialization gives
     * us insertion-order independence (proxy uses BTreeMap, same
     * guarantee — and goldlapel-ruby / goldlapel-python / goldlapel-js
     * mirror this with sorted-iteration). xxh64 picked for speed and
     * 64-bit width matching the proxy's u64; output is lowercase hex
     * (16 chars) for populated state, with the bare `"0"` sentinel for
     * empty state — by-construction non-collision since the lengths
     * differ.
     */
    private function recomputeStateHash(): void
    {
        if (empty($this->gucState)) {
            $this->gucStateHash = '0';
            return;
        }
        $sorted = $this->gucState;
        // SORT_STRING — explicit string-compare semantics so the order
        // stays identical to Ruby/Python/JS sorted iteration even if a
        // GUC value happens to be all-digits (which would otherwise
        // sort numerically under SORT_REGULAR).
        ksort($sorted, SORT_STRING);
        $serialized = '';
        foreach ($sorted as $k => $v) {
            $serialized .= $k . "\0" . $v . "\0";
        }
        $this->gucStateHash = hash('xxh64', $serialized);
    }
}
