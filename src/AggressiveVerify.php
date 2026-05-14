<?php

namespace GoldLapel;

/**
 * Aggressive-verify mode resolution for the post-DML cache-key bump.
 *
 * Background: the wrapper's GUC-RLS state-hash machinery (see
 * ConnectionGucState + observe-on-the-wire in CachedPDO) folds session-
 * GUC state into the L1 cache key so `SET app.user_id = '42'; SELECT
 * * FROM accounts` doesn't leak one user's rows to another's. The
 * wire layer catches `SET`, `SELECT set_config(...)`, and top-level
 * `SELECT my_function()`. It does NOT catch session-state mutations
 * issued INSIDE a trigger body fired by an INSERT/UPDATE/DELETE — the
 * wire layer never sees them.
 *
 * Aggressive verify closes that gap by bumping a per-connection
 * `dml_seq` counter after every observed DML statement. The counter
 * is mixed into the state hash so any cached pre-DML response from
 * this same connection cannot be served under a state that a server-
 * side trigger may have mutated. See `ConnectionGucState::bumpDmlSeq()`.
 *
 * Cost: zero round-trips, just a counter bump + hash recompute. This
 * is the v1 mitigation: cache-key isolation, not actual observation
 * of the new GUC values. PG itself always knows its own session state
 * and produces correct results; the proxy + wrapper just guarantee
 * the cache can't hand back a stale response keyed on pre-trigger
 * state.
 *
 * Mode semantics:
 *   - 'auto' (default) — bump on every DML. Same as 'on'; the two
 *     are kept distinct so a future "smart-detect-and-disable-when-
 *     unneeded" code path can flip 'auto' off without breaking users
 *     who explicitly pinned 'on'.
 *   - 'on' — bump on every DML. Identical to 'auto' today.
 *   - 'off' — opt-out. No bump. Emits an E_USER_WARNING on the first
 *     decide() call because the user is shrinking the correctness
 *     envelope (cached responses keyed on pre-DML state may be served
 *     even after a trigger SET).
 *
 * Pre-Wave-5 this module did MUCH more: a pg_trigger / pg_proc probe,
 * a license-payload bit, per-(database, process) detection caching,
 * and an inline `SELECT name, setting FROM pg_settings WHERE source=
 * 'session'` post-DML verify query. Wave 5 collapsed all of that into
 * a zero-RT counter bump on the wrapper side, matching the proxy's
 * `ConnectionGucState::mark_post_dml()` model. The detection probe
 * was pure paranoia management — the bump is cheap enough that doing
 * it unconditionally is the right default.
 */
final class AggressiveVerify
{
    public const MODE_AUTO = 'auto';
    public const MODE_ON = 'on';
    public const MODE_OFF = 'off';

    /**
     * Tracks whether the off-mode warning has fired for a given cache
     * key in this process. We warn ONCE per (connection-identity, mode)
     * combo so chatty applications that build many CachedPDO instances
     * against the same database don't spam stderr. Static so it survives
     * across CachedPDO instances within a single PHP process.
     *
     * @var array<string, true>
     */
    private static array $offWarned = [];

    /**
     * Resolve whether the post-DML bump should fire for this connection.
     *
     * @param string $mode  One of 'auto', 'on', 'off'. Anything else is
     *                      treated as 'auto' (lenient parsing — matches
     *                      the proxy's CLI flag parser).
     * @param string $cacheKey  Stable identity for the connection —
     *                          typically the upstream URL. Used to
     *                          deduplicate the off-mode warning.
     */
    public static function decide(string $mode, string $cacheKey): bool
    {
        $normalized = strtolower($mode);
        if ($normalized === self::MODE_OFF) {
            self::warnOffOnce($cacheKey);
            return false;
        }
        // Both 'auto' and 'on' bump. Anything unrecognised falls through
        // to bump as well — fail-safe toward correctness, matching the
        // proxy CLI's lenient parsing.
        return true;
    }

    /**
     * Emit the opt-out warning at most once per (cacheKey) in this
     * process. The user has explicitly disabled aggressive verify, so
     * cached responses keyed on pre-DML state may be served even after
     * a trigger SET — a correctness shrink they should be aware of.
     */
    private static function warnOffOnce(string $cacheKey): void
    {
        if (isset(self::$offWarned[$cacheKey])) {
            return;
        }
        self::$offWarned[$cacheKey] = true;
        @trigger_error(
            "GoldLapel: aggressive_verify='off' disables the post-DML "
            . "cache-key bump. Cached reads issued before a DML may be "
            . "served even when a server-side trigger has SET-mutated "
            . "session GUCs. Only safe when the schema has no triggers "
            . "that mutate session state.",
            E_USER_WARNING,
        );
    }

    /**
     * Reset the per-process "warned-once" tracker. Tests use this to
     * isolate warning behaviour between cases.
     */
    public static function clearCache(): void
    {
        self::$offWarned = [];
    }
}
