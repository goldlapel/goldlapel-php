<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for the startup banner:
 *   1. Banner must be written to stderr, not stdout. In a PHP-FPM / web
 *      context, stdout becomes the HTTP response body — a stray banner
 *      corrupts JSON output, injects whitespace before a <!doctype>, and
 *      triggers "headers already sent" errors.
 *   2. The 'silent' wrapper option suppresses the banner entirely.
 *   3. 'silent' is a wrapper-only config key and must NOT leak to the
 *      Rust binary as a CLI flag.
 */
class BannerTest extends TestCase
{
    /** @var resource|null */
    private $originalBannerStream;

    protected function setUp(): void
    {
        // Save whatever the class-level banner stream is so we can restore
        // it after each test. The property is static, so tests would
        // otherwise bleed into one another.
        $ref = new \ReflectionProperty(GoldLapel::class, 'bannerStream');
        $ref->setAccessible(true);
        $this->originalBannerStream = $ref->getValue();
    }

    protected function tearDown(): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'bannerStream');
        $ref->setAccessible(true);
        $ref->setValue(null, $this->originalBannerStream);
    }

    /**
     * Swap the class-level banner stream for a php://memory handle and
     * return both the handle (so the test can read from it) and a closer
     * (so the test can release it in finally).
     *
     * @return array{0: resource}
     */
    private function captureBanner(): array
    {
        $mem = fopen('php://memory', 'w+');
        $ref = new \ReflectionProperty(GoldLapel::class, 'bannerStream');
        $ref->setAccessible(true);
        $ref->setValue(null, $mem);
        return [$mem];
    }

    private function invokePrintBanner(int $port, int $dashboardPort, bool $silent): void
    {
        $ref = new \ReflectionMethod(GoldLapel::class, 'printBanner');
        $ref->setAccessible(true);
        $ref->invoke(null, $port, $dashboardPort, $silent);
    }

    // ------------------------------------------------------------------
    // 1. Banner routed to stderr, not stdout
    // ------------------------------------------------------------------

    public function testBannerWritesToStderrStreamNotStdout(): void
    {
        [$mem] = $this->captureBanner();

        // Use output buffering to prove NOTHING leaks to stdout.
        ob_start();
        try {
            $this->invokePrintBanner(7932, 7933, false);
        } finally {
            $stdout = ob_get_clean();
        }

        rewind($mem);
        $stderr = stream_get_contents($mem);

        $this->assertSame(
            '',
            $stdout,
            'Banner must not leak to stdout — in a web context this corrupts the HTTP response body.'
        );
        $this->assertStringContainsString('goldlapel', $stderr);
        $this->assertStringContainsString(':7932 (proxy)', $stderr);
        $this->assertStringContainsString('http://127.0.0.1:7933 (dashboard)', $stderr);
    }

    public function testBannerWithoutDashboardStillOnStderrOnly(): void
    {
        [$mem] = $this->captureBanner();

        ob_start();
        try {
            $this->invokePrintBanner(7932, 0, false);
        } finally {
            $stdout = ob_get_clean();
        }

        rewind($mem);
        $stderr = stream_get_contents($mem);

        $this->assertSame('', $stdout);
        $this->assertStringContainsString(':7932 (proxy)', $stderr);
        $this->assertStringNotContainsString('dashboard', $stderr);
    }

    // ------------------------------------------------------------------
    // 2. `silent` option suppresses the banner entirely
    // ------------------------------------------------------------------

    public function testSilentConfigSuppressesBanner(): void
    {
        [$mem] = $this->captureBanner();

        ob_start();
        try {
            $this->invokePrintBanner(7932, 7933, true);
        } finally {
            $stdout = ob_get_clean();
        }

        rewind($mem);
        $stderr = stream_get_contents($mem);

        $this->assertSame('', $stdout, 'Silent must not write to stdout.');
        $this->assertSame('', $stderr, 'Silent must not write to stderr either.');
    }

    public function testSilentDefaultsToFalse(): void
    {
        // parseStartOptions should return $silent = false when not specified.
        $ref = new \ReflectionMethod(GoldLapel::class, 'parseStartOptions');
        $ref->setAccessible(true);
        [$port, $config, $extraArgs, $silent] = $ref->invoke(null, []);
        $this->assertFalse($silent);
    }

    public function testSilentOptionParsedAsTrue(): void
    {
        $ref = new \ReflectionMethod(GoldLapel::class, 'parseStartOptions');
        $ref->setAccessible(true);
        [$port, $config, $extraArgs, $silent] = $ref->invoke(null, ['silent' => true]);
        $this->assertTrue($silent);
    }

    // ------------------------------------------------------------------
    // 3. `silent` must NOT be forwarded to the Rust binary as a CLI flag
    // ------------------------------------------------------------------

    public function testSilentNotForwardedToBinaryArgv(): void
    {
        $ref = new \ReflectionMethod(GoldLapel::class, 'parseStartOptions');
        $ref->setAccessible(true);
        [$port, $config, $extraArgs, $silent] = $ref->invoke(null, [
            'port' => 7932,
            'silent' => true,
            'mode' => 'waiter',
        ]);

        // Silent must not land in $config (from which CLI flags are built)
        // and must not land in $extraArgs.
        $this->assertArrayNotHasKey('silent', $config);
        $this->assertNotContains('--silent', $extraArgs);
        $this->assertNotContains('silent', $extraArgs);

        // And the full argv assembled by configToArgs() must not mention it.
        $args = GoldLapel::configToArgs($config);
        $this->assertNotContains('--silent', $args);
        $this->assertNotContains('silent', $args);

        // Sanity: real config keys still came through.
        $this->assertContains('--mode', $args);
        $this->assertContains('waiter', $args);
    }

    public function testSilentWithExplicitConfigArrayStillNotLeaked(): void
    {
        // Belt-and-braces: caller passes both a silent flag AND a 'config'
        // sub-array. The wrapper-only key must still be stripped.
        $ref = new \ReflectionMethod(GoldLapel::class, 'parseStartOptions');
        $ref->setAccessible(true);
        [$port, $config, $extraArgs, $silent] = $ref->invoke(null, [
            'silent' => true,
            'config' => ['mode' => 'waiter'],
        ]);

        $this->assertTrue($silent);
        $this->assertArrayNotHasKey('silent', $config);
        $this->assertSame(['mode' => 'waiter'], $config);

        $args = GoldLapel::configToArgs($config);
        $this->assertNotContains('--silent', $args);
    }

    public function testSilentFalseyValuesTreatedAsFalse(): void
    {
        // silent => false, 0, '', null should all yield $silent === false.
        $ref = new \ReflectionMethod(GoldLapel::class, 'parseStartOptions');
        $ref->setAccessible(true);

        foreach ([false, 0, '', null] as $falsey) {
            [$_p, $config, $_e, $silent] = $ref->invoke(null, ['silent' => $falsey]);
            $this->assertFalse($silent, "silent => " . var_export($falsey, true) . ' should be false');
            $this->assertArrayNotHasKey('silent', $config);
        }
    }
}
