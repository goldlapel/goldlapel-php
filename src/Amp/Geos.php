<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Geo namespace API — `$gl->geos-><verb>(...)` for the async surface.
 *
 * Phase 5 GEOGRAPHY-native schema. Mirrors `goldlapel.geos.GeosAPI` and
 * the sync `GoldLapel\Geos`.
 */
final class Geos
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        \GoldLapel\Utils::validateIdentifier($name);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'geo',
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

    public function add(string $name, string $member, float $lon, float $lat, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoAdd($c, $name, $member, $lon, $lat, $patterns));
    }

    public function pos(string $name, string $member, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoPos($c, $name, $member, $patterns));
    }

    public function dist(string $name, string $memberA, string $memberB, string $unit = 'm', ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoDist($c, $name, $memberA, $memberB, $unit, $patterns));
    }

    public function radius(string $name, float $lon, float $lat, float $radius, string $unit = 'm', int $limit = 50, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoRadius($c, $name, $lon, $lat, $radius, $unit, $limit, $patterns));
    }

    public function radiusByMember(string $name, string $member, float $radius, string $unit = 'm', int $limit = 50, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoRadiusByMember($c, $name, $member, $radius, $unit, $limit, $patterns));
    }

    public function remove(string $name, string $member, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoRemove($c, $name, $member, $patterns));
    }

    public function count(string $name, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::geoCount($c, $name, $patterns));
    }
}
