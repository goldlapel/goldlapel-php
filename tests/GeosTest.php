<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\Geos;
use GoldLapel\GoldLapel;
use GoldLapel\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Geos — the nested `$gl->geos` namespace.
 *
 * Phase 5 schema decisions:
 *   - GEOGRAPHY column type (not GEOMETRY) — distance returns are meters.
 *   - `member TEXT PRIMARY KEY` — re-adding a member updates its location
 *     (idempotent), matching Redis GEOADD semantics.
 *   - `updated_at` stamped on every UPSERT.
 *
 * Tests verify:
 *   - `add` is idempotent on member name (the proxy's ON CONFLICT DO UPDATE).
 *   - SQL uses the canonical GEOGRAPHY-native pattern (no `::geography`
 *     casts on the column reference because the column already IS geography).
 *   - Distance unit conversion at the wrapper edge (m / km / mi / ft).
 *   - radius binds [lon, lat, radius_m, limit] — proxy CTE-anchor contract.
 *   - radiusByMember binds [member, member, radius_m, limit].
 *
 * Mirrors goldlapel-python/tests/test_geos.py.
 */
#[AllowMockObjectsWithoutExpectations]
class GeosTest extends TestCase
{
    private function makeGl(): GoldLapel
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $pdo = $this->createMock(\PDO::class);
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        $ref->setValue($gl, $pdo);
        return $gl;
    }

    private function fakePatterns(): array
    {
        $main = '_goldlapel.geo_riders';
        return [
            'tables' => ['main' => $main],
            'query_patterns' => [
                'geoadd' => "INSERT INTO {$main} (member, location, updated_at) VALUES (\$1, ST_SetSRID(ST_MakePoint(\$2, \$3), 4326)::geography, NOW()) ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat",
                'geopos' => "SELECT ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM {$main} WHERE member = \$1",
                'geodist' => "SELECT ST_Distance(a.location, b.location) AS distance_m FROM {$main} a, {$main} b WHERE a.member = \$1 AND b.member = \$2",
                'georadius_with_dist' => "WITH anchor AS (SELECT ST_SetSRID(ST_MakePoint(\$1, \$2), 4326)::geography AS p) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat, ST_Distance(location, anchor.p) AS distance_m FROM {$main}, anchor WHERE ST_DWithin(location, anchor.p, \$3) ORDER BY distance_m LIMIT \$4",
                // Proxy contract: $1+$2 both bind the anchor member name (one
                // for the join, one for the self-exclusion), $3=radius_m,
                // $4=limit. Source order is $1,$2,$3,$4 — matching the
                // `[member, member, radius_m, limit]` bind contract.
                'geosearch_member' => "SELECT b.member, ST_X(b.location::geometry) AS lon, ST_Y(b.location::geometry) AS lat, ST_Distance(b.location, a.location) AS distance_m FROM {$main} a, {$main} b WHERE a.member = \$1 AND b.member <> \$2 AND ST_DWithin(b.location, a.location, \$3) ORDER BY distance_m LIMIT \$4",
                'geo_remove' => "DELETE FROM {$main} WHERE member = \$1",
                'geo_count' => "SELECT COUNT(*) FROM {$main}",
            ],
        ];
    }

    private function seedCache(GoldLapel $gl, string $family, string $name, array $patterns): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'ddlCache');
        $ref->setAccessible(true);
        $cache = $ref->getValue($gl);
        $cache["{$family}:{$name}"] = $patterns;
        $ref->setValue($gl, $cache);
    }

    private function getInternalPdo(GoldLapel $gl): \PDO
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    // ---- Namespace shape ----

    public function testGeosIsAGeosInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Geos::class, $gl->geos);
    }

    public function testNoLegacyFlatGeoMethods(): void
    {
        $gl = $this->makeGl();
        foreach (['geoadd', 'georadius', 'geodist'] as $name) {
            $this->assertFalse(method_exists($gl, $name));
        }
    }

    // ---- Verb dispatch ----

    public function testAddIsIdempotentViaOnConflict(): void
    {
        $sql = $this->fakePatterns()['query_patterns']['geoadd'];
        $this->assertStringContainsString('ON CONFLICT (member)', $sql);
        $this->assertStringContainsString('DO UPDATE', $sql);
    }

    public function testAddPatternIsGeographyNative(): void
    {
        $sql = $this->fakePatterns()['query_patterns']['geoadd'];
        $this->assertStringContainsStringIgnoringCase('geography', $sql);
    }

    public function testAddBindsMemberLonLat(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([13.4, 52.5]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['alice', 13.4, 52.5]);

        $result = $gl->geos->add('riders', 'alice', 13.4, 52.5);
        $this->assertSame([13.4, 52.5], $result);
    }

    public function testPosReturnsLonLat(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([13.4, 52.5]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame([13.4, 52.5], $gl->geos->pos('riders', 'alice'));
    }

    public function testPosReturnsNullForUnknownMember(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertNull($gl->geos->pos('riders', 'missing'));
    }

    public function testDistDefaultsToMeters(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1234.0);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(1234.0, $gl->geos->dist('riders', 'a', 'b'));
    }

    public function testDistConvertsToKm(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1234.0);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(1.234, $gl->geos->dist('riders', 'a', 'b', 'km'));
    }

    public function testDistConvertsToMiles(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1609.344);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertEqualsWithDelta(1.0, $gl->geos->dist('riders', 'a', 'b', 'mi'), 1e-6);
    }

    public function testDistUnknownUnitRaises(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1.0);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown distance unit');
        $gl->geos->dist('riders', 'a', 'b', 'parsec');
    }

    public function testRadiusBindsLonLatRadiusMLimit(): void
    {
        // Proxy contract: $1=lon, $2=lat, $3=radius_m, $4=limit. CTE anchor
        // means each $N appears exactly once in the rendered SQL — bind in
        // [lon, lat, radius_m, limit] order.
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with([13.4, 52.5, 5000.0, 50]);

        $gl->geos->radius('riders', 13.4, 52.5, 5, 'km');
    }

    public function testRadiusByMemberBindsMemberMemberRadiusLimit(): void
    {
        // Proxy contract: $1+$2 both bind the anchor member; $3=radius_m,
        // $4=limit. Bind [member, member, radius_m, limit].
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn([]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['alice', 'alice', 1000.0, 50]);

        $gl->geos->radiusByMember('riders', 'alice', 1000);
    }

    public function testRemoveReturnsTrueWhenDeleted(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertTrue($gl->geos->remove('riders', 'alice'));
    }

    public function testCountReturnsValue(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'geo', 'riders', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(3);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(3, $gl->geos->count('riders'));
    }
}
