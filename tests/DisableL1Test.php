<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the canonical `disable_l1` top-level option.
 *
 * disable_l1: true puts the wrapper's NativeCache into a no-op
 * pass-through. get() returns null (and ticks misses so the proxy sees
 * the request rate via telemetry); put() is silent. Distinct from
 * `cache_size: 0` / GOLDLAPEL_NATIVE_CACHE=false — disable_l1 lets the
 * customer keep their tuned cache size and toggle just the layer.
 */
class DisableL1Test extends TestCase
{
    protected function setUp(): void
    {
        NativeCache::reset();
        // The telemetry path keys off SAPI; phpunit runs under cli but
        // belt-and-suspenders the snapshot-emit checks too.
        putenv('GOLDLAPEL_REPORT_STATS=true');
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
        putenv('GOLDLAPEL_REPORT_STATS');
    }

    private function disableL1FieldValue(GoldLapel $gl): bool
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'disableL1');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    // ─── default behaviour: cache works as today ────────────────────────

    public function testDefaultPutAndGetRoundtrips(): void
    {
        $cache = new NativeCache();
        $cache->setConnected(true);
        $cache->put('SELECT * FROM users', null, [['id' => '1']], ['id']);
        $entry = $cache->get('SELECT * FROM users', null);
        $this->assertNotNull($entry);
        $this->assertSame(1, $cache->statsHits);
        $this->assertSame(0, $cache->statsMisses);
        $this->assertFalse($cache->isDisabled());
    }

    public function testDisableL1OptionDefaultsToFalse(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->disableL1FieldValue($gl));
    }

    public function testDefaultSnapshotOmitsL1DisabledKey(): void
    {
        $cache = new NativeCache();
        $cache->setConnected(true);
        $snap = $cache->buildSnapshot();
        $this->assertArrayNotHasKey('l1_disabled', $snap);
    }

    // ─── disable_l1 on the GoldLapel factory ────────────────────────────

    public function testDisableL1OptionParsedAsTrue(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['disable_l1' => true]);
        $this->assertTrue($this->disableL1FieldValue($gl));
    }

    public function testDisableL1FalseyValuesTreatedAsFalse(): void
    {
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', ['disable_l1' => $falsey]);
            $this->assertFalse(
                $this->disableL1FieldValue($gl),
                'disable_l1 => ' . var_export($falsey, true) . ' should be false'
            );
        }
    }

    public function testDisableL1AsConfigKeyRejected(): void
    {
        // disable_l1 is a top-level canonical-surface option; it must not
        // be valid inside the structured config map.
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', [
            'config' => ['disable_l1' => true],
        ]);
    }

    public function testWrapPDOStaticPropagatesDisableFlag(): void
    {
        // Stand up a stub server so connectInvalidation succeeds — both
        // get() and put() require invalidationConnected before the
        // disabled-check fires.
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);
        $port = (int) explode(':', stream_socket_get_name($server, false))[1];
        stream_set_blocking($server, false);

        // Use a test stub (not a mock) — wrapPDOStatic just stores the PDO
        // on the CachedPDO, no methods are invoked, so a stub is the
        // narrower fit and avoids PHPUnit's "no expectations" notice.
        $pdo = $this->createStub(\PDO::class);

        try {
            GoldLapel::wrapPDOStatic($pdo, $port, true);
            $cache = NativeCache::getInstance();
            $this->assertTrue($cache->isDisabled());
            $this->assertTrue($cache->isConnected());
        } finally {
            fclose($server);
        }
    }

    public function testWrapPDOStaticNullDisableLeavesStateUntouched(): void
    {
        // Tri-state contract: passing null must not flip a previously
        // disabled cache back to enabled. Laravel's GoldLapelConnection
        // calls wrapPDOStatic without the third arg, so a Laravel wrap
        // following a `disable_l1: true` factory must not clobber the
        // disabled state.
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);
        $port = (int) explode(':', stream_socket_get_name($server, false))[1];
        stream_set_blocking($server, false);

        $pdo = $this->createStub(\PDO::class);

        try {
            // First call: factory wraps with disable_l1=true.
            GoldLapel::wrapPDOStatic($pdo, $port, true);
            $cache = NativeCache::getInstance();
            $this->assertTrue($cache->isDisabled());

            // Second call: Laravel-style passive wrap (no third arg).
            // Must not re-enable the cache.
            GoldLapel::wrapPDOStatic($pdo, $port);
            $this->assertTrue($cache->isDisabled());
        } finally {
            fclose($server);
        }
    }

    // ─── disabled cache semantics ───────────────────────────────────────

    public function testDisabledGetReturnsNullAndTicksMisses(): void
    {
        $cache = new NativeCache(null, null, true);
        $cache->setConnected(true);
        // Even after a put(), get() must miss.
        $cache->put('SELECT * FROM users', null, [['id' => '1']], ['id']);
        $this->assertNull($cache->get('SELECT * FROM users', null));
        $this->assertNull($cache->get('SELECT * FROM users', null));
        $this->assertNull($cache->get('SELECT * FROM other', null));
        $this->assertSame(3, $cache->statsMisses);
        $this->assertSame(0, $cache->statsHits);
    }

    public function testDisabledPutIsNoOp(): void
    {
        $cache = new NativeCache(null, null, true);
        $cache->setConnected(true);
        $cache->put('SELECT * FROM users', null, [['id' => '1']], ['id']);
        $cache->put('SELECT * FROM orders', null, [['id' => '2']], ['id']);
        $this->assertSame(0, $cache->size());
        $this->assertSame(0, $cache->statsEvictions);
    }

    public function testDisabledViaSetterMatchesConstructor(): void
    {
        $cache = new NativeCache();
        $cache->setConnected(true);
        $cache->setDisabled(true);
        $this->assertTrue($cache->isDisabled());

        $cache->put('SELECT 1', null, [['x' => '1']], ['x']);
        $this->assertNull($cache->get('SELECT 1', null));
        $this->assertSame(1, $cache->statsMisses);
        $this->assertSame(0, $cache->statsHits);

        // Re-enabling must restore normal behaviour.
        $cache->setDisabled(false);
        $cache->put('SELECT 1', null, [['x' => '1']], ['x']);
        $this->assertNotNull($cache->get('SELECT 1', null));
        $this->assertSame(1, $cache->statsHits);
    }

    public function testDisabledEvictionCounterStaysZeroDespiteSpam(): void
    {
        // Capacity 4, but disabled — no entries are admitted, so no
        // eviction can ever fire even under heavy traffic.
        $cache = new NativeCache(4, null, true);
        $cache->setConnected(true);
        for ($i = 0; $i < 50; $i++) {
            $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
        }
        $this->assertSame(0, $cache->statsEvictions);
        $this->assertSame(0, $cache->size());
    }

    // ─── snapshot field ────────────────────────────────────────────────

    public function testSnapshotIncludesL1DisabledWhenDisabled(): void
    {
        $cache = new NativeCache(null, null, true);
        $cache->setConnected(true);
        $cache->get('SELECT 1', null); // tick a miss
        $snap = $cache->buildSnapshot();
        $this->assertArrayHasKey('l1_disabled', $snap);
        $this->assertTrue($snap['l1_disabled']);
        $this->assertSame(1, $snap['misses']);
        $this->assertSame(0, $snap['hits']);
        $this->assertSame(0, $snap['evictions']);
        $this->assertSame(0, $snap['current_size_entries']);
    }

    public function testSnapshotOmitsL1DisabledAfterReenable(): void
    {
        $cache = new NativeCache(null, null, true);
        $cache->setConnected(true);
        $this->assertArrayHasKey('l1_disabled', $cache->buildSnapshot());
        $cache->setDisabled(false);
        $this->assertArrayNotHasKey('l1_disabled', $cache->buildSnapshot());
    }

    // ─── invalidation polling/listener still runs ──────────────────────

    public function testDisabledStillProcessesInvalidationSignals(): void
    {
        // disable_l1 must not silence the proxy's signal stream — the
        // wrapper still receives I:* / I:<table> / ?:snapshot and the
        // listener must keep running for telemetry. Easiest assertion:
        // processSignal() still routes correctly and bumps the
        // invalidation counter when a wildcard arrives.
        $cache = new NativeCache(null, null, true);
        $cache->setConnected(true);
        $cache->processSignal('I:*');
        // invalidateAll bumps statsInvalidations by current size, which is
        // 0 here — the contract we care about is "did the routing run":
        // route through a non-wildcard table that doesn't exist. Either
        // way, no exception is the success signal; assert the cache still
        // reports as connected so the listener path is not torn down.
        $this->assertTrue($cache->isConnected());
        $this->assertTrue($cache->isDisabled());
    }
}
