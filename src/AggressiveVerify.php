<?php

namespace GoldLapel;

/**
 * Smart-auto-enable for the post-DML verify pass.
 *
 * Background: the wrapper's GUC-RLS state-hash machinery (see
 * ConnectionGucState + post-call verify in CachedPDO) is observe-on-the-
 * wire — it catches `SET app.user_id = …` and `SELECT set_config(…)` and
 * top-level `SELECT my_function()` calls. It does NOT catch session-state
 * mutations issued INSIDE a trigger body fired by an INSERT/UPDATE/DELETE,
 * because the wire layer never sees them.
 *
 * Aggressive verify closes that gap by running the verify-on-checkout
 * query (the same pg_settings probe the dirty-flag fallback uses) after
 * every DML statement. Cost is ~1ms per write — high enough that we don't
 * want it on by default, low enough that it's worth turning on when a
 * schema actually has trigger-internal SETs.
 *
 * This class supplies three things:
 *   1. Detection — a lightweight pg_trigger / pg_proc probe that returns
 *      true iff the database has at least one user trigger whose function
 *      body contains a `SET <namespaced-guc>` or `set_config(...)` call.
 *   2. Caching — the probe runs once per (database, FPM worker / CLI
 *      invocation). Static class-level cache keyed by an externally-
 *      supplied identity string (typically the proxy URL or DSN).
 *   3. Decision — given the user's override (`auto`/`on`/`off`), the
 *      license-payload hint (read from `GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE`
 *      if exposed by the proxy), and the auto-detection result, return a
 *      boolean for "should aggressive verify run".
 *
 * Precedence (highest to lowest):
 *   - explicit override: `'on'` → true, `'off'` → false
 *   - license-payload bit: `GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=true|false`
 *   - auto-detection on the connection
 *
 * PHP request-scoped semantics: the cache lives for the lifetime of the
 * FPM worker (or CLI invocation). That's the natural unit of "first
 * connection" — long-running PHP processes (Octane/Swoole/RoadRunner, CLI
 * daemons) reuse the same detection result across requests, while a
 * stock FPM worker re-detects per worker, which is fine because the
 * trigger inventory is stable.
 *
 * @internal The `auto` decision path is intentionally fail-safe: if the
 *           probe query throws (permissions, network blip, schema with
 *           pg_proc locked down), we fall back to OFF rather than ON.
 *           Aggressive verify is paranoia mode; failing closed preserves
 *           the no-tax default.
 */
final class AggressiveVerify
{
    public const MODE_AUTO = 'auto';
    public const MODE_ON = 'on';
    public const MODE_OFF = 'off';

    /**
     * Per-(database, process) detection cache. Keys are caller-supplied
     * identity strings; values are bool (true = trigger-internal SETs
     * detected, false = no triggers seen). Static so it survives across
     * CachedPDO instances within a single PHP process.
     *
     * @var array<string, bool>
     */
    private static array $detectionCache = [];

    /**
     * SQL probe — returns one row with `present` = bool. Looks for any
     * non-internal trigger whose function body either:
     *   - issues `SET <something>.<something>` (a namespaced GUC, the
     *     RLS-typical shape), OR
     *   - calls `set_config(...)` (with or without `pg_catalog.` prefix).
     *
     * The regex is deliberately liberal — false positives just turn on
     * aggressive verify (a paranoid tax of ~1ms per write), never silently
     * drop a real trigger SET. False negatives are the security risk, so
     * we err toward catching too much.
     *
     * Uses `~*` for case-insensitive POSIX regex (standard Postgres). No
     * dependency on `regexp_matches` or other extensions. Safe across all
     * PG versions ≥ 9.5.
     */
    public const DETECTION_SQL = <<<'SQL'
SELECT EXISTS (
    SELECT 1
      FROM pg_trigger t
      JOIN pg_proc p ON t.tgfoid = p.oid
     WHERE NOT t.tgisinternal
       AND (
            p.prosrc ~* '\\mset[[:space:]]+[a-z_][a-z0-9_]*\\.[a-z_]'
         OR p.prosrc ~* '\\mset_config[[:space:]]*\\('
       )
) AS present
SQL;

    /**
     * Resolve "should aggressive verify run on this connection?".
     *
     * @param string $mode  One of 'auto', 'on', 'off'. Anything else is
     *                      treated as 'auto' (lenient parsing).
     * @param string $cacheKey  Stable identity for the (database, process)
     *                          pair — typically the proxy URL or the PDO
     *                          DSN. The auto-detect result is cached on
     *                          this key so repeated CachedPDO instances
     *                          against the same database don't re-probe.
     * @param callable(): bool $detector  Closure that runs the probe and
     *                                    returns the detection result.
     *                                    Invoked at most once per cacheKey
     *                                    in 'auto' mode.
     */
    public static function decide(string $mode, string $cacheKey, callable $detector): bool
    {
        // Step 1 — explicit user override wins. 'on' / 'off' bypass both
        // the license payload and auto-detection. This matches the
        // documented precedence: a user who set `aggressive_verify => 'on'`
        // expects it to be on regardless of what the proxy thinks.
        $normalized = strtolower($mode);
        if ($normalized === self::MODE_ON) {
            return true;
        }
        if ($normalized === self::MODE_OFF) {
            return false;
        }

        // Step 2 — license-payload hint, if exposed. The proxy may
        // surface the customer's setting (from the HQ Settings tab,
        // baked into the license payload as `aggressive_verify_active`)
        // via the env var below. Today the env var is unset; this branch
        // is forward-compatible with a future proxy that exposes it.
        // 'true' / '1' / 'yes' all coerce to on. Anything else (including
        // 'false') is treated as the customer NOT having opted in via the
        // payload — fall through to auto.
        $licenseHint = getenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
        if (is_string($licenseHint) && $licenseHint !== '') {
            $hintLower = strtolower($licenseHint);
            if (in_array($hintLower, ['true', '1', 'yes', 'on'], true)) {
                return true;
            }
            if (in_array($hintLower, ['false', '0', 'no', 'off'], true)) {
                return false;
            }
            // Unrecognized value — fall through to auto rather than
            // silently honouring it. Mirrors the `mode` lenient parsing.
        }

        // Step 3 — auto-detection, cached per (database, process). Run
        // the detector at most once per cacheKey — trigger inventory is
        // stable within a worker's lifetime and the probe touches system
        // catalogs (cheap, but not free).
        if (!array_key_exists($cacheKey, self::$detectionCache)) {
            try {
                self::$detectionCache[$cacheKey] = (bool) $detector();
            } catch (\Throwable $e) {
                // Fail-safe: on any probe error, treat as "no triggers
                // detected" (off). Aggressive verify is paranoia mode;
                // failing closed preserves the no-tax default rather than
                // silently turning on a write tax the user can't explain.
                self::$detectionCache[$cacheKey] = false;
            }
        }
        return self::$detectionCache[$cacheKey];
    }

    /**
     * Detector factory for sync PDO. Returns a closure that runs the
     * probe SQL via the supplied PDO and yields the boolean result.
     */
    public static function pdoDetector(\PDO $pdo): \Closure
    {
        return static function () use ($pdo): bool {
            $stmt = $pdo->query(self::DETECTION_SQL);
            if ($stmt === false) {
                return false;
            }
            $row = $stmt->fetch(\PDO::FETCH_ASSOC);
            if (!is_array($row)) {
                return false;
            }
            $val = $row['present'] ?? null;
            return self::coerceBool($val);
        };
    }

    /**
     * Stable cache key derived from a PDO. We can't read the DSN back out
     * of a constructed PDO (PDO::ATTR_CONNECTION_STATUS gives a vendor
     * blob, not the DSN), so callers should prefer passing an explicit
     * `cacheKey` derived from their own connection string. This helper
     * is the fallback — it uses spl_object_hash so each PDO gets its own
     * detection slot; that's a worst-case-correct default (re-probes per
     * connection) and matches the dispatch's "per-database via DSN"
     * intent for most setups where each PDO is one-to-one with a
     * database.
     */
    public static function defaultCacheKey(\PDO $pdo): string
    {
        return 'pdo:' . spl_object_hash($pdo);
    }

    /**
     * Reset the static detection cache. Tests use this to isolate
     * detection behaviour between cases. Production code shouldn't call
     * this — the cache is process-scoped by design.
     */
    public static function clearCache(): void
    {
        self::$detectionCache = [];
    }

    /**
     * Inspect the cache for a given key. Returns true / false if the
     * detector has run for this key, or null if it hasn't yet. Exposed
     * for tests + telemetry.
     */
    public static function cached(string $cacheKey): ?bool
    {
        return self::$detectionCache[$cacheKey] ?? null;
    }

    /**
     * Loose boolean coercion — Postgres returns the EXISTS predicate as
     * `t` / `f` over the wire by default, but pdo_pgsql sometimes maps
     * to PHP true/false directly. Accept both, plus the usual string
     * variants, plus integer 0/1.
     */
    private static function coerceBool(mixed $val): bool
    {
        if (is_bool($val)) {
            return $val;
        }
        if (is_int($val)) {
            return $val !== 0;
        }
        if (is_string($val)) {
            $lower = strtolower($val);
            return in_array($lower, ['t', 'true', '1', 'yes', 'on'], true);
        }
        return false;
    }
}
