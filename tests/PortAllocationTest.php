<?php

namespace GoldLapel\Tests;

use GoldLapel\Amp\GoldLapel as AmpGoldLapel;
use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Port allocation for several proxies in one process.
 *
 * Each proxy holds two ports: proxy P and dashboard P+1 (unless
 * `dashboard_port` is explicit; 0 disables it). Without an explicit
 * `proxy_port`, a proxy takes the smallest P >= 7932 such that neither P nor
 * its dashboard port is held by another live proxy this process started —
 * sync and Amp share one ledger. An explicit `proxy_port` is used as given
 * and still counts as held. Stopping releases.
 */
class PortAllocationTest extends TestCase
{
    protected function setUp(): void
    {
        $this->resetClaims();
    }

    protected function tearDown(): void
    {
        GoldLapel::cleanupAll();
        AmpGoldLapel::cleanupAll();
        $this->resetClaims();
    }

    private function resetClaims(): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'claimedPorts');
        $ref->setAccessible(true);
        $ref->setValue(null, []);
    }

    // ─── the ledger ────────────────────────────────────────────────────

    public function testFirstProxyGetsDefaultPorts(): void
    {
        $this->assertSame([7932, 7933], GoldLapel::claimPorts(1, null, null));
    }

    public function testSecondProxySkipsFirstProxysDashboard(): void
    {
        GoldLapel::claimPorts(1, null, null);
        $this->assertSame([7934, 7935], GoldLapel::claimPorts(2, null, null));
        $this->assertSame([7936, 7937], GoldLapel::claimPorts(3, null, null));
    }

    public function testReleaseFreesPortsForReuse(): void
    {
        GoldLapel::claimPorts(1, null, null);
        GoldLapel::claimPorts(2, null, null);
        GoldLapel::releasePorts(1);
        $this->assertSame([7932, 7933], GoldLapel::claimPorts(3, null, null));
    }

    public function testExplicitProxyPortCountsAsClaimed(): void
    {
        $this->assertSame([7932, 7933], GoldLapel::claimPorts(1, 7932, null));
        $this->assertSame([7934, 7935], GoldLapel::claimPorts(2, null, null));
    }

    public function testExplicitPortUsedAsGivenEvenWhenClaimed(): void
    {
        GoldLapel::claimPorts(1, null, null);
        $this->assertSame([7933, 7934], GoldLapel::claimPorts(2, 7933, null));
    }

    public function testExplicitDashboardPortCountsAsClaimed(): void
    {
        GoldLapel::claimPorts(1, 9000, 7934);
        // 7932/7933 are free; 7934 is held by the first proxy's dashboard.
        $this->assertSame([7932, 7933], GoldLapel::claimPorts(2, null, null));
        // 7934 is taken, so 7935/7936 is the next free pair after 7932/7933.
        $this->assertSame([7935, 7936], GoldLapel::claimPorts(3, null, null));
    }

    public function testDisabledDashboardClaimsOnlyProxyPort(): void
    {
        $this->assertSame([7932, 0], GoldLapel::claimPorts(1, null, 0));
        $this->assertSame([7933, 7934], GoldLapel::claimPorts(2, null, null));
    }

    public function testAutoPortWithExplicitDashboardOnlyNeedsProxyPortFree(): void
    {
        GoldLapel::claimPorts(1, null, null);
        $this->assertSame([7934, 9000], GoldLapel::claimPorts(2, null, 9000));
    }

    public function testAutoPortNeverCollidesWithItsOwnDashboard(): void
    {
        $this->assertSame([7933, 7932], GoldLapel::claimPorts(1, null, 7932));
    }

    public function testReclaimBySameOwnerReplacesItsClaim(): void
    {
        GoldLapel::claimPorts(1, null, null);
        GoldLapel::claimPorts(1, 9000, null);
        $this->assertSame([7932, 7933], GoldLapel::claimPorts(2, null, null));
    }

    // ─── through the factories ─────────────────────────────────────────

    public function testSyncAndAmpProxiesShareTheLedger(): void
    {
        $restore = $this->useFakeBinary();
        try {
            $a = GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', ['silent' => true]);
            $b = GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/b', ['silent' => true]);
            $c = AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/c', ['silent' => true])->await();

            $this->assertSame([7932, 7933], [$a->getProxyPort(), $a->getDashboardPort()]);
            $this->assertSame([7934, 7935], [$b->getProxyPort(), $b->getDashboardPort()]);
            $this->assertSame([7936, 7937], [$c->getProxyPort(), $c->getDashboardPort()]);
            $this->assertStringContainsString(':7934/', (string) $b->url());

            // Stopping releases: the next proxy reuses the freed pair.
            $a->stop();
            $d = AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/d', ['silent' => true])->await();
            $this->assertSame(7932, $d->getProxyPort());

            $d->stop()->await();
            $e = GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/e', ['silent' => true]);
            $this->assertSame(7932, $e->getProxyPort());
        } finally {
            $restore();
        }
    }

    public function testFailedStartReleasesItsPorts(): void
    {
        $restore = $this->useFakeBinary("#!/bin/sh\nexit 1\n");
        try {
            try {
                GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', ['silent' => true]);
                $this->fail('start must throw when the binary exits');
            } catch (\RuntimeException $e) {
            }
            try {
                AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/b', ['silent' => true])->await();
                $this->fail('start must throw when the binary exits');
            } catch (\RuntimeException $e) {
            }
            $this->assertSame([7932, 7933], GoldLapel::claimPorts(1, null, null));
        } finally {
            $restore();
        }
    }

    /**
     * Point GOLDLAPEL_BINARY at a fake that binds whatever --proxy-port it is
     * given, so the factory's readiness check only passes on the allocated
     * port. Returns a restore callback.
     */
    private function useFakeBinary(?string $script = null): callable
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH — required for fake binary');
        }

        $script ??= "#!/bin/sh\n"
            . "port=\n"
            . "while [ \$# -gt 0 ]; do\n"
            . "  if [ \"\$1\" = --proxy-port ]; then port=\$2; fi\n"
            . "  shift\n"
            . "done\n"
            . "exec python3 -c \"import socket,sys,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',int(sys.argv[1])))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\" \"\$port\"\n";
        $fake = tempnam(sys_get_temp_dir(), 'gl_ports_fake_');
        file_put_contents($fake, $script);
        chmod($fake, 0755);

        $origBinary = getenv('GOLDLAPEL_BINARY');
        putenv("GOLDLAPEL_BINARY={$fake}");

        return function () use ($origBinary, $fake): void {
            GoldLapel::cleanupAll();
            AmpGoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
            @unlink($fake);
        };
    }
}
