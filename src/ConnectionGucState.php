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
     * Read-only accessor for the current state hash. Hex-encoded
     * lowercase 64-bit value, or `"0"` for empty state.
     */
    public function stateHash(): string
    {
        return $this->gucStateHash;
    }

    /**
     * Observe a SQL string and apply any SET / RESET commands it contains
     * to this connection's unsafe-GUC state. Multi-statement bodies are
     * split on top-level `;` (string-literal-aware) so a single Q like
     * `SET app.user_id = '42'; SELECT 1` still updates state.
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
     * Apply a parsed SET / RESET command to this connection's unsafe-GUC
     * state. No-op for `set_local` (transient — cache is gated on
     * transaction-idle), no-op for safe GUC names. Recomputes the cached
     * state hash on mutation.
     *
     * @param array{type: string, name: ?string, value: ?string} $cmd
     */
    private function applySetCommand(array $cmd): void
    {
        switch ($cmd['type']) {
            case 'set':
                if ($cmd['name'] !== null && NativeCache::isUnsafeGuc($cmd['name'])) {
                    $this->gucState[$cmd['name']] = (string) $cmd['value'];
                    $this->recomputeStateHash();
                }
                break;
            case 'set_local':
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
                if (!empty($this->gucState)) {
                    $this->gucState = [];
                    $this->recomputeStateHash();
                }
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
