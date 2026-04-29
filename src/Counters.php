<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Counters namespace API — `$gl->counters-><verb>(...)`.
 *
 * Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:
 *
 *   1. Calls `/api/ddl/counter/create` (idempotent) to materialize the
 *      canonical `_goldlapel.counter_<name>` table and pull its query
 *      patterns.
 *   2. Caches `(tables, query_patterns)` on the parent GoldLapel instance
 *      for the session's lifetime (one HTTP round-trip per (family, name)).
 *   3. Hands the patterns off to the `Utils::counter*` helpers, which
 *      execute against the canonical table name.
 *
 * Mirrors goldlapel-python's `goldlapel.counters.CountersAPI`.
 *
 * Phase-5 contract: the proxy stamps `updated_at` on every UPDATE — the
 * `incr` and `set` patterns reference `NOW()`. Wrappers don't paper over
 * this; the column is part of the canonical schema.
 */
final class Counters
{
    public function __construct(private readonly GoldLapel $gl) {}

    /**
     * Fetch (and cache) canonical counter DDL + query patterns from the
     * proxy. Cache lives on the parent GoldLapel instance.
     */
    private function patterns(string $name): array
    {
        Utils::validateIdentifier($name);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'counter',
            $name,
            $this->gl->getDashboardPort(),
            $token,
        );
    }

    /**
     * Eagerly materialize the counter table. Other methods will also
     * materialize on first use, so calling this is optional — provided for
     * callers that want explicit setup at startup time.
     */
    public function create(string $name): void
    {
        $this->patterns($name);
    }

    public function incr(string $name, string $key, int $amount = 1, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::counterIncr(
            $this->gl->resolveConnPublic($conn), $name, $key, $amount, $patterns,
        );
    }

    public function decr(string $name, string $key, int $amount = 1, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::counterDecr(
            $this->gl->resolveConnPublic($conn), $name, $key, $amount, $patterns,
        );
    }

    public function set(string $name, string $key, int $value, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::counterSet(
            $this->gl->resolveConnPublic($conn), $name, $key, $value, $patterns,
        );
    }

    public function get(string $name, string $key, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::counterGet(
            $this->gl->resolveConnPublic($conn), $name, $key, $patterns,
        );
    }

    public function delete(string $name, string $key, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::counterDelete(
            $this->gl->resolveConnPublic($conn), $name, $key, $patterns,
        );
    }

    public function countKeys(string $name, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::counterCountKeys(
            $this->gl->resolveConnPublic($conn), $name, $patterns,
        );
    }
}
