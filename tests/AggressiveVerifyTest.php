<?php

namespace GoldLapel\Tests;

use GoldLapel\AggressiveVerify;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for the standalone AggressiveVerify decision module.
 *
 * Three layers exercised:
 *   1. Decision precedence: explicit override > license payload > auto.
 *   2. Detection caching: probe runs at most once per cache key.
 *   3. PDO-backed detector: maps the EXISTS-predicate row to a bool
 *      tolerantly across pdo_pgsql's two return shapes.
 *
 * Post-DML wiring on CachedPDO and Amp\CachedConnection lives in
 * tests/AggressiveVerifyWiringTest.php (sync) and
 * tests/Amp/AggressiveVerifyTest.php (async) — those depend on the
 * module being already in place, so they're tested separately.
 *
 * The license-payload path is exercised via the
 * GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE env var, which is the proxy's
 * forward-compatible hook — set / unset around each assertion.
 */
class AggressiveVerifyTest extends TestCase
{
    protected function setUp(): void
    {
        AggressiveVerify::clearCache();
        // Ensure no leftover env var from previous tests poisons the
        // license-payload precedence assertions.
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
    }

    protected function tearDown(): void
    {
        AggressiveVerify::clearCache();
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE');
    }

    // ─── Decision precedence ───────────────────────────────────────────

    public function testExplicitOnAlwaysWins(): void
    {
        $detector = function (): bool {
            $this->fail('detector should not run when mode is "on"');
        };
        $this->assertTrue(AggressiveVerify::decide('on', 'k', $detector));
    }

    public function testExplicitOffAlwaysWins(): void
    {
        $detector = function (): bool {
            $this->fail('detector should not run when mode is "off"');
        };
        $this->assertFalse(AggressiveVerify::decide('off', 'k', $detector));
    }

    public function testExplicitOverrideBeatsLicensePayload(): void
    {
        // Even if the proxy says "active=true" via the license payload,
        // an explicit "off" from the user wins.
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=true');
        $this->assertFalse(AggressiveVerify::decide('off', 'k', fn() => true));

        // And vice-versa — explicit "on" beats payload "false".
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=false');
        $this->assertTrue(AggressiveVerify::decide('on', 'k', fn() => false));
    }

    public function testLicensePayloadTrueEnablesInAutoMode(): void
    {
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=true');
        $detector = function (): bool {
            $this->fail('detector should not run when license payload says true');
        };
        $this->assertTrue(AggressiveVerify::decide('auto', 'k', $detector));
    }

    public function testLicensePayloadFalseDisablesInAutoMode(): void
    {
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=false');
        $detector = function (): bool {
            $this->fail('detector should not run when license payload says false');
        };
        $this->assertFalse(AggressiveVerify::decide('auto', 'k', $detector));
    }

    public function testLicensePayloadAcceptsCommonTruthyForms(): void
    {
        foreach (['true', 'TRUE', '1', 'yes', 'on', 'On'] as $value) {
            putenv("GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE={$value}");
            $this->assertTrue(
                AggressiveVerify::decide('auto', "k-{$value}", fn() => false),
                "value '{$value}' should be coerced to true",
            );
        }
    }

    public function testLicensePayloadAcceptsCommonFalsyForms(): void
    {
        foreach (['false', 'FALSE', '0', 'no', 'off', 'Off'] as $value) {
            putenv("GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE={$value}");
            $this->assertFalse(
                AggressiveVerify::decide('auto', "k-{$value}", fn() => true),
                "value '{$value}' should be coerced to false",
            );
        }
    }

    public function testLicensePayloadUnrecognisedValueFallsThroughToAuto(): void
    {
        // A bogus payload value (e.g. a future schema we don't know) is
        // treated as "no override" — fall through to the detector. This
        // is forward-compat: the proxy can ship new sentinel values
        // without breaking older wrappers.
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=maybe');
        $this->assertTrue(AggressiveVerify::decide('auto', 'k', fn() => true));
        $this->assertFalse(AggressiveVerify::decide('auto', 'k2', fn() => false));
    }

    public function testEmptyLicensePayloadFallsThroughToAuto(): void
    {
        // putenv with no value does NOT clear; setting to empty string
        // is treated as "unset".
        putenv('GOLDLAPEL_AGGRESSIVE_VERIFY_ACTIVE=');
        $this->assertTrue(AggressiveVerify::decide('auto', 'k', fn() => true));
    }

    public function testAutoModeUsesDetector(): void
    {
        $this->assertTrue(AggressiveVerify::decide('auto', 'k1', fn() => true));
        $this->assertFalse(AggressiveVerify::decide('auto', 'k2', fn() => false));
    }

    public function testUnrecognisedModeIsTreatedAsAuto(): void
    {
        // Lenient parsing — anything other than 'on' / 'off' falls
        // through to auto. Avoids surprising users who type 'true' or
        // mis-spell.
        $this->assertTrue(AggressiveVerify::decide('garbage', 'k', fn() => true));
        $this->assertFalse(AggressiveVerify::decide('garbage', 'k2', fn() => false));
    }

    public function testModeIsCaseInsensitive(): void
    {
        $this->assertTrue(AggressiveVerify::decide('ON', 'k', fn() => false));
        $this->assertTrue(AggressiveVerify::decide('On', 'k2', fn() => false));
        $this->assertFalse(AggressiveVerify::decide('OFF', 'k3', fn() => true));
    }

    // ─── Detection caching ─────────────────────────────────────────────

    public function testDetectorRunsOnceAndCaches(): void
    {
        $calls = 0;
        $detector = function () use (&$calls): bool {
            $calls++;
            return true;
        };
        // First call: runs detector.
        AggressiveVerify::decide('auto', 'cache-key-1', $detector);
        $this->assertSame(1, $calls);

        // Second call (same key): cache hit, detector NOT called.
        AggressiveVerify::decide('auto', 'cache-key-1', $detector);
        $this->assertSame(1, $calls);

        // Different key: detector runs again.
        AggressiveVerify::decide('auto', 'cache-key-2', $detector);
        $this->assertSame(2, $calls);
    }

    public function testCachedReturnsNullBeforeFirstDecision(): void
    {
        $this->assertNull(AggressiveVerify::cached('never-asked'));
    }

    public function testCachedReturnsResultAfterDecision(): void
    {
        AggressiveVerify::decide('auto', 'k-true', fn() => true);
        AggressiveVerify::decide('auto', 'k-false', fn() => false);
        $this->assertTrue(AggressiveVerify::cached('k-true'));
        $this->assertFalse(AggressiveVerify::cached('k-false'));
    }

    public function testClearCacheResetsState(): void
    {
        AggressiveVerify::decide('auto', 'k', fn() => true);
        $this->assertTrue(AggressiveVerify::cached('k'));
        AggressiveVerify::clearCache();
        $this->assertNull(AggressiveVerify::cached('k'));
    }

    public function testDetectorThrowFallsBackToOff(): void
    {
        // Fail-safe: a probe error (permissions, network, etc.) caches
        // FALSE rather than retrying forever or crashing the wrapper.
        // Aggressive verify is paranoia mode; failing closed preserves
        // the no-tax default.
        $detector = function (): bool {
            throw new \RuntimeException('network down');
        };
        $this->assertFalse(AggressiveVerify::decide('auto', 'k', $detector));
        // Cached as false, so subsequent calls don't re-run.
        $this->assertFalse(AggressiveVerify::cached('k'));
    }

    public function testExplicitOnIsNotCached(): void
    {
        // Explicit overrides bypass the detection cache entirely — they
        // never populate it. This matters if a user flips 'on' → 'off'
        // mid-session: the next decide() with 'auto' would still see a
        // fresh detector, not a stale "true" entry.
        AggressiveVerify::decide('on', 'k', fn() => false);
        $this->assertNull(AggressiveVerify::cached('k'));
    }

    // ─── PDO detector probe ────────────────────────────────────────────

    public function testPdoDetectorReturnsTrueOnPresentRow(): void
    {
        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetch')->willReturn(['present' => true]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_trigger'))
            ->willReturn($stmt);
        $detector = AggressiveVerify::pdoDetector($pdo);
        $this->assertTrue($detector());
    }

    public function testPdoDetectorReturnsFalseOnAbsentRow(): void
    {
        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetch')->willReturn(['present' => false]);
        $pdo = $this->createStub(\PDO::class);
        $pdo->method('query')->willReturn($stmt);
        $detector = AggressiveVerify::pdoDetector($pdo);
        $this->assertFalse($detector());
    }

    public function testPdoDetectorAcceptsStringTrue(): void
    {
        // Postgres' EXISTS predicate returns 't'/'f' over the wire by
        // default; pdo_pgsql sometimes hands those through as strings.
        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetch')->willReturn(['present' => 't']);
        $pdo = $this->createStub(\PDO::class);
        $pdo->method('query')->willReturn($stmt);
        $detector = AggressiveVerify::pdoDetector($pdo);
        $this->assertTrue($detector());
    }

    public function testPdoDetectorReturnsFalseOnQueryFailure(): void
    {
        $pdo = $this->createStub(\PDO::class);
        $pdo->method('query')->willReturn(false);
        $detector = AggressiveVerify::pdoDetector($pdo);
        $this->assertFalse($detector());
    }

    public function testDefaultCacheKeyIsStablePerPdo(): void
    {
        $pdo1 = $this->createStub(\PDO::class);
        $pdo2 = $this->createStub(\PDO::class);
        $this->assertSame(
            AggressiveVerify::defaultCacheKey($pdo1),
            AggressiveVerify::defaultCacheKey($pdo1),
        );
        $this->assertNotSame(
            AggressiveVerify::defaultCacheKey($pdo1),
            AggressiveVerify::defaultCacheKey($pdo2),
        );
    }
}
