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
 * sync and Amp share one ledger — and both can be bound at the OS level. An
 * explicit port another live proxy holds raises; one another program holds
 * is left for the proxy to refuse. Stopping, a failed start and the proxy
 * exiting all release.
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

    /** @var list<object> keeps the stand-ins alive: the ledger holds them weakly */
    private array $owners = [];

    /** A stand-in for a proxy instance: claimPorts() only asks isRunning(). */
    private function owner(bool $running = true): object
    {
        return $this->owners[] = new class ($running) {
            public function __construct(public bool $running)
            {
            }

            public function isRunning(): bool
            {
                return $this->running;
            }
        };
    }

    private function claim(object $owner, ?int $proxyPort = null, ?int $dashboardPort = null, string $upstream = 'postgresql://u:secret@db/a'): array
    {
        return GoldLapel::claimPorts($owner, $upstream, $proxyPort, $dashboardPort);
    }

    public function testFirstProxyGetsDefaultPorts(): void
    {
        $this->assertSame([7932, 7933], $this->claim($this->owner()));
    }

    public function testSecondProxySkipsFirstProxysDashboard(): void
    {
        $this->claim($this->owner());
        $this->assertSame([7934, 7935], $this->claim($this->owner()));
        $this->assertSame([7936, 7937], $this->claim($this->owner()));
    }

    public function testReleaseFreesPortsForReuse(): void
    {
        $first = $this->owner();
        $this->claim($first);
        $this->claim($this->owner());
        GoldLapel::releasePorts($first);
        $this->assertSame([7932, 7933], $this->claim($this->owner()));
    }

    public function testExplicitProxyPortCountsAsClaimed(): void
    {
        $this->assertSame([7932, 7933], $this->claim($this->owner(), 7932));
        $this->assertSame([7934, 7935], $this->claim($this->owner()));
    }

    public function testExplicitPortHeldByAnotherProxyRaises(): void
    {
        $this->claim($this->owner(), null, null, 'postgresql://app:hunter2@db1/app');
        try {
            $this->claim($this->owner(), 7933);
            $this->fail('An explicit port another live proxy holds must raise.');
        } catch (\RuntimeException $e) {
            $this->assertStringContainsString('port 7933 as the proxy port', $e->getMessage());
            $this->assertStringContainsString('postgresql://app:***@db1/app', $e->getMessage());
            $this->assertStringContainsString('as its dashboard port', $e->getMessage());
            $this->assertStringNotContainsString('hunter2', $e->getMessage());
        }
    }

    public function testExplicitPortWhoseDashboardIsHeldRaises(): void
    {
        $this->claim($this->owner());
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('port 7932 as the dashboard port');
        // proxy 7931 is free, but its dashboard (7932) is the first proxy's.
        $this->claim($this->owner(), 7931);
    }

    public function testExplicitDashboardPortHeldByAnotherProxyRaises(): void
    {
        $this->claim($this->owner());
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('port 7933 as the dashboard port');
        $this->claim($this->owner(), null, 7933);
    }

    public function testFailedClaimHoldsNothing(): void
    {
        $this->claim($this->owner(), 9000);
        try {
            $this->claim($this->owner(), 9000);
        } catch (\RuntimeException $e) {
        }
        $this->assertSame([7932, 7933], $this->claim($this->owner()));
    }

    public function testExitedProxyHoldsNothing(): void
    {
        $dead = $this->owner();
        $this->claim($dead);
        $dead->running = false;
        $this->assertSame([7932, 7933], $this->claim($this->owner()));
        // An explicit port is free again too.
        $this->assertSame([7934, 7935], $this->claim($this->owner(), 7934));
    }

    public function testExplicitDashboardPortCountsAsClaimed(): void
    {
        $this->claim($this->owner(), 9000, 7934);
        // 7932/7933 are free; 7934 is held by the first proxy's dashboard.
        $this->assertSame([7932, 7933], $this->claim($this->owner()));
        // 7934 is taken, so 7935/7936 is the next free pair after 7932/7933.
        $this->assertSame([7935, 7936], $this->claim($this->owner()));
    }

    public function testDisabledDashboardClaimsOnlyProxyPort(): void
    {
        $this->assertSame([7932, 0], $this->claim($this->owner(), null, 0));
        $this->assertSame([7933, 7934], $this->claim($this->owner()));
    }

    public function testAutoPortWithExplicitDashboardOnlyNeedsProxyPortFree(): void
    {
        $this->claim($this->owner());
        $this->assertSame([7934, 9000], $this->claim($this->owner(), null, 9000));
    }

    public function testAutoPortNeverCollidesWithItsOwnDashboard(): void
    {
        $this->assertSame([7933, 7932], $this->claim($this->owner(), null, 7932));
    }

    public function testReclaimBySameOwnerReplacesItsClaim(): void
    {
        $owner = $this->owner();
        $this->claim($owner);
        $this->claim($owner, 9000);
        $this->assertSame([7932, 7933], $this->claim($this->owner()));
    }

    // ─── ports other programs hold ─────────────────────────────────────

    public function testAutoPortSkipsAPortBusyAtTheOsLevel(): void
    {
        $busy = $this->listenOn(7932);
        try {
            // 7932 can't be bound; 7933 and 7934 can.
            $this->assertSame([7933, 7934], $this->claim($this->owner()));
        } finally {
            fclose($busy);
        }
    }

    public function testAutoPortSkipsAPairWhoseDashboardIsBusyAtTheOsLevel(): void
    {
        $busy = $this->listenOn(7933);
        try {
            $this->assertSame([7934, 7935], $this->claim($this->owner()));
        } finally {
            fclose($busy);
        }
    }

    public function testExplicitPortBusyAtTheOsLevelIsLeftToTheProxy(): void
    {
        $busy = $this->listenOn(7932);
        try {
            $this->assertSame([7932, 7933], $this->claim($this->owner(), 7932));
        } finally {
            fclose($busy);
        }
    }

    /** @return resource */
    private function listenOn(int $port)
    {
        $server = @stream_socket_server("tcp://0.0.0.0:{$port}");
        if ($server === false) {
            $this->markTestSkipped("port {$port} is already in use on this host");
        }
        return $server;
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
            $this->assertSame([7932, 7933], $this->claim($this->owner()));
        } finally {
            $restore();
        }
    }

    public function testStartThatFailsBeforeTheSpawnHoldsNothing(): void
    {
        $restore = $this->useFakeBinary();
        try {
            $bad = [
                ['log_level' => 'loud'],
                ['config' => ['disable_pool' => 'yes']],
                ['invalidation_port' => 7934],
            ];
            foreach ($bad as $options) {
                foreach ([
                    fn () => GoldLapel::start('postgresql://u:p@localhost:5432/a', $options),
                    fn () => GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', $options),
                    fn () => AmpGoldLapel::start('postgresql://u:p@localhost:5432/a', $options)->await(),
                    fn () => AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', $options)->await(),
                ] as $start) {
                    try {
                        $start();
                        $this->fail('start must reject ' . json_encode($options));
                    } catch (\InvalidArgumentException | \TypeError $e) {
                    }
                }
            }
            $this->assertSame([7932, 7933], $this->claim($this->owner()));
        } finally {
            $restore();
        }
    }

    public function testExplicitPortAnotherProxyHoldsRaisesBeforeSpawning(): void
    {
        $restore = $this->useFakeBinary();
        try {
            $a = GoldLapel::startProxyOnly('postgresql://u:hunter2@localhost:5432/a', ['silent' => true]);
            foreach ([
                fn () => GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/b', ['proxy_port' => 7933, 'silent' => true]),
                fn () => AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/b', ['proxy_port' => 7932, 'silent' => true])->await(),
            ] as $start) {
                try {
                    $start();
                    $this->fail('An explicit port the running proxy holds must raise.');
                } catch (\RuntimeException $e) {
                    $this->assertStringContainsString('postgresql://u:***@localhost:5432/a', $e->getMessage());
                }
            }
            $this->assertTrue($a->isRunning(), 'The proxy holding the port must be left alone.');
            $this->assertSame([7934, 7935], $this->claim($this->owner()));
        } finally {
            $restore();
        }
    }

    public function testSameUpstreamReusesTheRunningProxy(): void
    {
        $restore = $this->useFakeBinary();
        try {
            $url = 'postgresql://u:p@localhost:5432/a';
            $a = GoldLapel::startProxyOnly($url, ['silent' => true]);
            $b = GoldLapel::startProxyOnly($url, ['silent' => true, 'proxy_port' => 9100]);
            $this->assertSame($a, $b, 'Same upstream must reuse the running proxy.');
            $this->assertSame(7932, $b->getProxyPort());

            // The first holder's stop() leaves the proxy to the second.
            $a->stop();
            $this->assertTrue($b->isRunning());
            $this->assertNotNull($b->url());
            $this->assertSame([7934, 7935], $this->claim($this->owner()));

            $b->stop();
            $this->assertFalse($b->isRunning());
            $b->stop(); // idempotent past the last holder
        } finally {
            $restore();
        }
    }

    public function testSameUpstreamReusesTheRunningProxyAmp(): void
    {
        $restore = $this->useFakeBinary();
        try {
            $url = 'postgresql://u:p@localhost:5432/a';
            $a = AmpGoldLapel::startProxyOnly($url, ['silent' => true])->await();
            $b = AmpGoldLapel::startProxyOnly($url, ['silent' => true])->await();
            $this->assertSame($a, $b, 'Same upstream must reuse the running proxy.');

            $a->stop()->await();
            $this->assertTrue($b->isRunning());
            $b->stop()->await();
            $this->assertFalse($b->isRunning());
        } finally {
            $restore();
        }
    }

    public function testStartErrorCarriesTheProxysExitStatusAndStderr(): void
    {
        $restore = $this->useFakeBinary(
            "#!/bin/sh\necho \"I'm afraid port 7932, for the proxy, is already in use\" >&2\nexit 3\n"
        );
        try {
            foreach ([
                fn () => GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', ['silent' => true]),
                fn () => AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', ['silent' => true])->await(),
            ] as $start) {
                try {
                    $start();
                    $this->fail('start must throw when the proxy exits');
                } catch (\RuntimeException $e) {
                    $this->assertStringContainsString('exited with status 3', $e->getMessage());
                    $this->assertStringContainsString("I'm afraid port 7932", $e->getMessage());
                }
            }
        } finally {
            $restore();
        }
    }

    public function testAPortThatAnsweredBeforeTheSpawnIsNotReadiness(): void
    {
        // Something else already listens on the explicit port. The port
        // answering says nothing about our proxy, which refuses it.
        $busy = $this->listenOn(7950);
        $restore = $this->useFakeBinary(
            "#!/bin/sh\nsleep 0.3\necho \"I'm afraid port 7950, for the proxy, is already in use\" >&2\nexit 1\n"
        );
        try {
            foreach ([
                fn () => GoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', ['proxy_port' => 7950, 'silent' => true]),
                fn () => AmpGoldLapel::startProxyOnly('postgresql://u:p@localhost:5432/a', ['proxy_port' => 7950, 'silent' => true])->await(),
            ] as $start) {
                try {
                    $start();
                    $this->fail('A port another program holds must not pass as ready.');
                } catch (\RuntimeException $e) {
                    $this->assertStringContainsString("I'm afraid port 7950", $e->getMessage());
                }
            }
        } finally {
            fclose($busy);
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
