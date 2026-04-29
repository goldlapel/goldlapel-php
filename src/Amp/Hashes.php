<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Hash namespace API — `$gl->hashes-><verb>(...)` for the async surface.
 *
 * Phase 5 row-per-field schema. Mirrors `goldlapel.hashes.HashesAPI` and
 * the sync `GoldLapel\Hashes`.
 */
final class Hashes
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        \GoldLapel\Utils::validateIdentifier($name);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'hash',
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

    public function set(string $name, string $hashKey, string $field, mixed $value, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashSet($c, $name, $hashKey, $field, $value, $patterns));
    }

    public function get(string $name, string $hashKey, string $field, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashGet($c, $name, $hashKey, $field, $patterns));
    }

    public function getAll(string $name, string $hashKey, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashGetAll($c, $name, $hashKey, $patterns));
    }

    public function keys(string $name, string $hashKey, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashKeys($c, $name, $hashKey, $patterns));
    }

    public function values(string $name, string $hashKey, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashValues($c, $name, $hashKey, $patterns));
    }

    public function exists(string $name, string $hashKey, string $field, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashExists($c, $name, $hashKey, $field, $patterns));
    }

    public function delete(string $name, string $hashKey, string $field, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashDelete($c, $name, $hashKey, $field, $patterns));
    }

    public function len(string $name, string $hashKey, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::hashLen($c, $name, $hashKey, $patterns));
    }
}
