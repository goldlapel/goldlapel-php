<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Counters namespace API — `$gl->counters-><verb>(...)` for the async surface.
 *
 * Phase 5 of schema-to-core. Mirrors goldlapel-python's
 * `goldlapel.counters.CountersAPI` and the sync `GoldLapel\Counters`.
 */
final class Counters
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        \GoldLapel\Utils::validateIdentifier($name);
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

    public function create(string $name): Future
    {
        return async(function () use ($name): void {
            $this->patterns($name);
        });
    }

    public function incr(string $name, string $key, int $amount = 1, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::counterIncr($c, $name, $key, $amount, $patterns));
    }

    public function decr(string $name, string $key, int $amount = 1, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::counterDecr($c, $name, $key, $amount, $patterns));
    }

    public function set(string $name, string $key, int $value, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::counterSet($c, $name, $key, $value, $patterns));
    }

    public function get(string $name, string $key, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::counterGet($c, $name, $key, $patterns));
    }

    public function delete(string $name, string $key, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::counterDelete($c, $name, $key, $patterns));
    }

    public function countKeys(string $name, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::counterCountKeys($c, $name, $patterns));
    }
}
