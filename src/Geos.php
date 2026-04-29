<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Geo namespace API — `$gl->geos-><verb>(...)`.
 *
 * Phase 5 of schema-to-core. The proxy's v1 geo schema uses GEOGRAPHY (not
 * GEOMETRY), `member TEXT PRIMARY KEY` (not `BIGSERIAL` + `name`), and a
 * GIST index on the location column. `geos->add` is idempotent on the
 * member name — re-adding a member updates its location.
 *
 * Distance unit: methods accept `$unit = 'm' | 'km' | 'mi' | 'ft'`. The
 * proxy column is meters-native (GEOGRAPHY default); wrappers convert at
 * the edge.
 *
 * Mirrors goldlapel-python's `goldlapel.geos.GeosAPI`.
 */
final class Geos
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        Utils::validateIdentifier($name);
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

    public function create(string $name): void
    {
        $this->patterns($name);
    }

    public function add(string $name, string $member, float $lon, float $lat, ?\PDO $conn = null): ?array
    {
        $patterns = $this->patterns($name);
        return Utils::geoAdd(
            $this->gl->resolveConnPublic($conn), $name, $member, $lon, $lat, $patterns,
        );
    }

    public function pos(string $name, string $member, ?\PDO $conn = null): ?array
    {
        $patterns = $this->patterns($name);
        return Utils::geoPos(
            $this->gl->resolveConnPublic($conn), $name, $member, $patterns,
        );
    }

    public function dist(string $name, string $memberA, string $memberB, string $unit = 'm', ?\PDO $conn = null): ?float
    {
        $patterns = $this->patterns($name);
        return Utils::geoDist(
            $this->gl->resolveConnPublic($conn), $name, $memberA, $memberB, $unit, $patterns,
        );
    }

    public function radius(string $name, float $lon, float $lat, float $radius, string $unit = 'm', int $limit = 50, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($name);
        return Utils::geoRadius(
            $this->gl->resolveConnPublic($conn), $name, $lon, $lat, $radius, $unit, $limit, $patterns,
        );
    }

    public function radiusByMember(string $name, string $member, float $radius, string $unit = 'm', int $limit = 50, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($name);
        return Utils::geoRadiusByMember(
            $this->gl->resolveConnPublic($conn), $name, $member, $radius, $unit, $limit, $patterns,
        );
    }

    public function remove(string $name, string $member, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::geoRemove(
            $this->gl->resolveConnPublic($conn), $name, $member, $patterns,
        );
    }

    public function count(string $name, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::geoCount(
            $this->gl->resolveConnPublic($conn), $name, $patterns,
        );
    }
}
