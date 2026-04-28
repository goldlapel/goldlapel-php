<?php

namespace GoldLapel\Tests;

use GoldLapel\Amp\GoldLapel as AmpGoldLapel;
use GoldLapel\GoldLapel as SyncGoldLapel;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionMethod;

/**
 * Parity test: GoldLapel\GoldLapel (sync) vs GoldLapel\Amp\GoldLapel (async).
 *
 * Both classes are hand-written method-by-method — every public, non-static
 * method on the sync surface has a matching method on the Amp surface (and
 * vice versa) with an identical parameter signature. The async body delegates
 * to GoldLapel\Amp\Utils inside an `async()` fiber and returns Future<T>; the
 * sync body delegates to the static GoldLapel\Utils helpers and returns the
 * value directly. Wrapper-method shapes (name, positional args, defaults,
 * `?PostgresExecutor $conn = null` / `?\PDO $conn = null` overrides) match.
 *
 * This test exists to catch the silent-drift class of bug: someone adds a
 * new wrapper method to one surface and forgets the other. Pure name-set
 * comparison plus parameter-name + default-value equality is enough to flag
 * every case we've seen.
 *
 * Methods that legitimately exist on only one side are listed in
 * SYNC_ONLY / AMP_ONLY below with a comment explaining why each one differs.
 * The test should fail loudly until the skip is documented — drift
 * discipline beats convenience.
 */
class AsyncParityTest extends TestCase
{
    /**
     * Sync-only methods. These reflect surface differences that are
     * intentional, not drift.
     *
     * Type-specific accessors (PDO is the sync-only driver):
     *   - pdo, pdoDsn, pdoCredentials, wrapPDO
     *
     * Internal accessors used by tests / Ddl helpers — Amp's class accesses
     * the same private state directly without needing a public accessor:
     *   - dashboardToken, ddlCache
     */
    private const SYNC_ONLY = [
        'pdo', 'pdoDsn', 'pdoCredentials', 'wrapPDO',
        'dashboardToken', 'ddlCache',
    ];

    /**
     * Amp-only methods. These reflect surface differences that are
     * intentional, not drift.
     *
     * Type-specific accessors (amphp PostgresConnection is async-only):
     *   - connect, connection — open / fetch the amphp connection
     *   - wrapCached         — sync analogue is wrapPDO (different driver type)
     */
    private const AMP_ONLY = [
        'connect', 'connection', 'wrapCached',
    ];

    /**
     * Public, non-static, non-magic instance methods declared directly on
     * the class (not inherited).
     *
     * @return string[]
     */
    private function publicMethods(string $class): array
    {
        $rc = new ReflectionClass($class);
        $names = [];
        foreach ($rc->getMethods(ReflectionMethod::IS_PUBLIC) as $m) {
            if ($m->getDeclaringClass()->getName() !== $class) {
                continue;
            }
            if ($m->isStatic() || $m->isConstructor() || $m->isDestructor()) {
                continue;
            }
            // Skip magic methods (__call, __invoke, etc.) — they're framework
            // hooks, not part of the wrapper surface.
            if (str_starts_with($m->getName(), '__')) {
                continue;
            }
            $names[] = $m->getName();
        }
        sort($names);
        return $names;
    }

    public function testSyncMethodsPresentOnAmp(): void
    {
        $sync = $this->publicMethods(SyncGoldLapel::class);
        $amp = $this->publicMethods(AmpGoldLapel::class);
        $missing = array_values(array_diff($sync, $amp, self::SYNC_ONLY));
        $this->assertSame(
            [],
            $missing,
            "Methods on sync GoldLapel but missing from Amp GoldLapel: " .
            implode(', ', $missing) . ". Either add the method to the Amp " .
            "surface, or add it to SYNC_ONLY in this test with a comment " .
            "explaining why.",
        );
    }

    public function testAmpMethodsPresentOnSync(): void
    {
        $sync = $this->publicMethods(SyncGoldLapel::class);
        $amp = $this->publicMethods(AmpGoldLapel::class);
        $missing = array_values(array_diff($amp, $sync, self::AMP_ONLY));
        $this->assertSame(
            [],
            $missing,
            "Methods on Amp GoldLapel but missing from sync GoldLapel: " .
            implode(', ', $missing) . ". Either add the method to the sync " .
            "surface, or add it to AMP_ONLY in this test with a comment " .
            "explaining why.",
        );
    }

    public function testMethodSignaturesMatch(): void
    {
        $sync = $this->publicMethods(SyncGoldLapel::class);
        $amp = $this->publicMethods(AmpGoldLapel::class);
        $shared = array_values(array_intersect($sync, $amp));
        $drifted = [];
        foreach ($shared as $name) {
            $syncSig = $this->signature(SyncGoldLapel::class, $name);
            $ampSig = $this->signature(AmpGoldLapel::class, $name);
            if ($syncSig !== $ampSig) {
                $drifted[$name] = ['sync' => $syncSig, 'amp' => $ampSig];
            }
        }
        if (!empty($drifted)) {
            $report = '';
            foreach ($drifted as $name => $sigs) {
                $report .= "\n  {$name}\n";
                $report .= "    sync: " . json_encode($sigs['sync']) . "\n";
                $report .= "    amp:  " . json_encode($sigs['amp']) . "\n";
            }
            $this->fail("Method signatures drifted between sync and Amp surfaces:" . $report);
        } else {
            $this->assertTrue(true);
        }
    }

    /**
     * Capture the part of a method's signature that should match across
     * sync/amp: parameter names, optionality, default values, by-reference,
     * variadic-ness, nullability. The async surface's return type is
     * `Future` while the sync surface's return type is the unwrapped T —
     * that's the *intended* difference, so we deliberately exclude return
     * types from this comparison.
     *
     * Parameter *types* also legitimately differ for the connection
     * override (sync: `?\PDO $conn`, amp: `?PostgresExecutor $conn`), so
     * we exclude type names too. What we keep is the structural shape:
     * name, hasDefault, default value, byRef, variadic.
     *
     * @return list<array<string, mixed>>
     */
    private function signature(string $class, string $method): array
    {
        $rm = new ReflectionMethod($class, $method);
        $sig = [];
        foreach ($rm->getParameters() as $p) {
            $sig[] = [
                'name' => $p->getName(),
                'optional' => $p->isOptional(),
                'default' => $p->isDefaultValueAvailable() ? $p->getDefaultValue() : null,
                'byRef' => $p->isPassedByReference(),
                'variadic' => $p->isVariadic(),
            ];
        }
        return $sig;
    }
}
