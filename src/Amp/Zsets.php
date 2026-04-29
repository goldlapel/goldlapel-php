<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Sorted-set (zset) namespace API — `$gl->zsets-><verb>(...)` for the async
 * surface. Mirrors `goldlapel.zsets.ZsetsAPI` and the sync `GoldLapel\Zsets`.
 */
final class Zsets
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        \GoldLapel\Utils::validateIdentifier($name);
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

    public function create(string $name): Future
    {
        return async(function () use ($name): void {
            $this->patterns($name);
        });
    }

    public function add(string $name, string $zsetKey, string $member, float $score, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetAdd($c, $name, $zsetKey, $member, $score, $patterns));
    }

    public function incrBy(string $name, string $zsetKey, string $member, float $delta = 1.0, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetIncrBy($c, $name, $zsetKey, $member, $delta, $patterns));
    }

    public function score(string $name, string $zsetKey, string $member, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetScore($c, $name, $zsetKey, $member, $patterns));
    }

    public function rank(string $name, string $zsetKey, string $member, bool $desc = true, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetRank($c, $name, $zsetKey, $member, $desc, $patterns));
    }

    public function range(string $name, string $zsetKey, int $start = 0, int $stop = -1, bool $desc = true, ?PostgresExecutor $conn = null): Future
    {
        if ($stop === -1) {
            $stop = 9999;
        }
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetRange($c, $name, $zsetKey, $start, $stop, $desc, $patterns));
    }

    public function rangeByScore(string $name, string $zsetKey, float $minScore, float $maxScore, int $limit = 100, int $offset = 0, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetRangeByScore($c, $name, $zsetKey, $minScore, $maxScore, $limit, $offset, $patterns));
    }

    public function remove(string $name, string $zsetKey, string $member, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetRemove($c, $name, $zsetKey, $member, $patterns));
    }

    public function card(string $name, string $zsetKey, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::zsetCard($c, $name, $zsetKey, $patterns));
    }
}
