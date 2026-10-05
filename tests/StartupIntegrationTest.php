<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\Amp\GoldLapel as AmpGoldLapel;
use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Start-up against the real proxy binary: port allocation around ports
 * other programs hold, the proxy's own refusal surfacing in the error, and
 * one shared proxy per upstream.
 *
 * Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM (see
 * IntegrationGate) and GOLDLAPEL_BINARY.
 */
class StartupIntegrationTest extends TestCase
{
    private static ?string $upstream = null;

    public static function setUpBeforeClass(): void
    {
        self::$upstream = IntegrationGate::upstream();
    }

    protected function setUp(): void
    {
        if (self::$upstream === null) {
            $this->markTestSkipped(IntegrationGate::skipReason());
        }
        $binary = getenv('GOLDLAPEL_BINARY');
        if (!$binary || !is_file($binary)) {
            $this->markTestSkipped('Set GOLDLAPEL_BINARY to a goldlapel binary to run this test.');
        }
    }

    protected function tearDown(): void
    {
        GoldLapel::cleanupAll();
        AmpGoldLapel::cleanupAll();
    }

    public function testExplicitPortAnotherProgramHoldsFailsWithTheProxysMessage(): void
    {
        [$busy, $port] = $this->listenOnFreePort();
        try {
            foreach ([
                fn () => GoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true]),
                fn () => AmpGoldLapel::start(self::$upstream, ['proxy_port' => $port, 'silent' => true])->await(),
            ] as $start) {
                try {
                    $start();
                    $this->fail("start() must fail when port {$port} is already taken.");
                } catch (\RuntimeException $e) {
                    $this->assertStringContainsString('exited with status 1', $e->getMessage());
                    $this->assertStringContainsString("port {$port}, for the proxy, is already in use", $e->getMessage());
                }
            }
        } finally {
            fclose($busy);
        }
    }

    public function testAllocationSkipsAPortAnotherProgramHolds(): void
    {
        $busy = @stream_socket_server('tcp://0.0.0.0:' . GoldLapel::DEFAULT_PROXY_PORT);
        if ($busy === false) {
            $this->markTestSkipped('port ' . GoldLapel::DEFAULT_PROXY_PORT . ' is already in use on this host');
        }
        try {
            $gl = GoldLapel::start(self::$upstream, ['silent' => true]);
            $this->assertNotSame(GoldLapel::DEFAULT_PROXY_PORT, $gl->getProxyPort());
            $this->assertSame(1, (int) $gl->pdo()->query('SELECT 1')->fetchColumn());
            $gl->stop();
        } finally {
            fclose($busy);
        }
    }

    public function testSameUpstreamSharesOneProxy(): void
    {
        $a = GoldLapel::startProxyOnly(self::$upstream, ['silent' => true]);
        $b = GoldLapel::start(self::$upstream, ['silent' => true]);
        $this->assertSame($a, $b);

        // The connection-less holder stops; the other keeps a working proxy.
        $a->stop();
        $this->assertSame(1, (int) $b->pdo()->query('SELECT 1')->fetchColumn());

        $b->stop();
        $this->assertFalse($b->isRunning());
    }

    /** @return array{0: resource, 1: int} */
    private function listenOnFreePort(): array
    {
        $server = stream_socket_server('tcp://0.0.0.0:0');
        $port = (int) substr((string) strrchr(stream_socket_get_name($server, false), ':'), 1);
        return [$server, $port];
    }
}
