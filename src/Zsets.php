<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Sorted-set (zset) namespace API — `$gl->zsets-><verb>(...)`.
 *
 * Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
 * `zset_key` column so a single namespace table holds many sorted sets,
 * matching Redis's mental model. Every method here threads `$zsetKey` as
 * the first positional arg after the namespace `$name`.
 *
 * Mirrors goldlapel-python's `goldlapel.zsets.ZsetsAPI`.
 */
final class Zsets
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        Utils::validateIdentifier($name);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'zset',
            $name,
            $this->gl->getDashboardPort(),
            $token,
        );
    }

    public function create(string $name): void
    {
        $this->patterns($name);
    }

    public function add(string $name, string $zsetKey, string $member, float $score, ?\PDO $conn = null): float
    {
        $patterns = $this->patterns($name);
        return Utils::zsetAdd(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $member, $score, $patterns,
        );
    }

    public function incrBy(string $name, string $zsetKey, string $member, float $delta = 1.0, ?\PDO $conn = null): float
    {
        $patterns = $this->patterns($name);
        return Utils::zsetIncrBy(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $member, $delta, $patterns,
        );
    }

    public function score(string $name, string $zsetKey, string $member, ?\PDO $conn = null): ?float
    {
        $patterns = $this->patterns($name);
        return Utils::zsetScore(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $member, $patterns,
        );
    }

    public function rank(string $name, string $zsetKey, string $member, bool $desc = true, ?\PDO $conn = null): ?int
    {
        $patterns = $this->patterns($name);
        return Utils::zsetRank(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $member, $desc, $patterns,
        );
    }

    /**
     * Members by rank within `$zsetKey`. `$start` and `$stop` are inclusive
     * Redis-style; `$stop=-1` is a sentinel meaning "to the end" — we map
     * it to a large limit (9999) since the proxy's pattern is LIMIT/OFFSET-
     * based. Callers wanting the full set should page via `rangeByScore`.
     */
    public function range(string $name, string $zsetKey, int $start = 0, int $stop = -1, bool $desc = true, ?\PDO $conn = null): array
    {
        if ($stop === -1) {
            $stop = 9999;
        }
        $patterns = $this->patterns($name);
        return Utils::zsetRange(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $start, $stop, $desc, $patterns,
        );
    }

    public function rangeByScore(string $name, string $zsetKey, float $minScore, float $maxScore, int $limit = 100, int $offset = 0, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($name);
        return Utils::zsetRangeByScore(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $minScore, $maxScore, $limit, $offset, $patterns,
        );
    }

    public function remove(string $name, string $zsetKey, string $member, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::zsetRemove(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $member, $patterns,
        );
    }

    public function card(string $name, string $zsetKey, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::zsetCard(
            $this->gl->resolveConnPublic($conn), $name, $zsetKey, $patterns,
        );
    }
}
