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

    private function silentFieldValue(GoldLapel $gl): bool
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'silent');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    private function configFieldValue(GoldLapel $gl): array
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'config');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    public function testSilentDefaultsToFalse(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->silentFieldValue($gl));
    }

    public function testSilentOptionParsedAsTrue(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['silent' => true]);
        $this->assertTrue($this->silentFieldValue($gl));
    }

    // ------------------------------------------------------------------
    // 3. `silent` must NOT be forwarded to the Rust binary as a CLI flag
    // ------------------------------------------------------------------

    public function testSilentNotForwardedToBinaryArgv(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [
            'proxy_port' => 7932,
            'silent' => true,
            'mode' => 'waiter',
        ]);

        // The configToArgs output for the structured config map must not
        // mention 'silent' — it's a wrapper-only key stored on the instance.
        $args = GoldLapel::configToArgs($this->configFieldValue($gl));
        $this->assertNotContains('--silent', $args);
        $this->assertNotContains('silent', $args);
    }

    public function testSilentAsConfigKeyRejected(): void
    {
        // Belt-and-braces: a caller who tries to pass 'silent' inside the
        // config sub-array must be rejected. 'silent' is a top-level
        // wrapper option, not a tuning knob — forwarding it as a CLI flag
        // to the Rust binary (which has no --silent) is a bug waiting to
        // happen.
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', [
            'config' => ['silent' => true],
        ]);
    }

    public function testSilentFalseyValuesTreatedAsFalse(): void
    {
        // silent => false, 0, '', null should all yield $silent === false.
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', ['silent' => $falsey]);
            $this->assertFalse(
                $this->silentFieldValue($gl),
                'silent => ' . var_export($falsey, true) . ' should be false'
            );
        }
    }

    // ─── Mesh startup options ─────────────────────────────────────────────

    private function meshFieldValue(GoldLapel $gl): bool
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'mesh');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    private function meshTagFieldValue(GoldLapel $gl): ?string
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'meshTag');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    public function testMeshDefaultsToFalse(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->meshFieldValue($gl));
        $this->assertNull($this->meshTagFieldValue($gl));
    }

    public function testMeshOptionParsedAsTrue(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['mesh' => true, 'mesh_tag' => 'prod-east']);
        $this->assertTrue($this->meshFieldValue($gl));
        $this->assertSame('prod-east', $this->meshTagFieldValue($gl));
    }

    public function testMeshTagEmptyStringNormalizedToNull(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['mesh' => true, 'mesh_tag' => '']);
        $this->assertNull($this->meshTagFieldValue($gl));
    }

    public function testMeshAsConfigKeyRejected(): void
    {
        // Belt-and-braces: mesh is a top-level canonical-surface option, never
        // valid inside the structured config map.
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', [
            'config' => ['mesh' => true],
        ]);
    }

    public function testMeshTagAsConfigKeyRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', [
            'config' => ['mesh_tag' => 'prod'],
        ]);
    }

    public function testMeshNotForwardedAsConfigArg(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [
            'mesh' => true,
            'mesh_tag' => 'prod-east',
        ]);
        // configToArgs only sees the structured config map; mesh lives
        // on the instance and is emitted in startProxyWithoutConnect.
        $args = GoldLapel::configToArgs($this->configFieldValue($gl));
        $this->assertNotContains('--mesh', $args);
        $this->assertNotContains('--mesh-tag', $args);
    }

    public function testMeshFalseyValuesTreatedAsFalse(): void
    {
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', ['mesh' => $falsey]);
            $this->assertFalse(
                $this->meshFieldValue($gl),
                'mesh => ' . var_export($falsey, true) . ' should be false'
            );
        }
    }

    // ─── enable_l2_for_wrappers startup option ────────────────────────────
    //
    // Per-connection L2 wrapper-skip is the global default — wrappers carry
    // an L1 cache and tag their PG application_name so the proxy bypasses L2
    // for them. Fleet customers (multi-pod, frequent restarts, mesh) flip
    // this on so the proxy's shared L2 keeps adding value across pods.

    private function enableL2ForWrappersFieldValue(GoldLapel $gl): bool
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'enableL2ForWrappers');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    public function testEnableL2ForWrappersDefaultsToFalse(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->enableL2ForWrappersFieldValue($gl));
    }

    public function testEnableL2ForWrappersOptionParsedAsTrue(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['enable_l2_for_wrappers' => true]);
        $this->assertTrue($this->enableL2ForWrappersFieldValue($gl));
    }

    public function testEnableL2ForWrappersAsConfigKeyRejected(): void
    {
        // Belt-and-braces: enable_l2_for_wrappers is a top-level canonical-
        // surface option, never valid inside the structured config map.
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', [
            'config' => ['enable_l2_for_wrappers' => true],
        ]);
    }

    public function testEnableL2ForWrappersNotForwardedAsConfigArg(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [
            'enable_l2_for_wrappers' => true,
        ]);
        // configToArgs only sees the structured config map; the wrapper-L2
        // flag lives on the instance and is emitted in
        // startProxyWithoutConnect.
        $args = GoldLapel::configToArgs($this->configFieldValue($gl));
        $this->assertNotContains('--enable-l2-for-wrappers', $args);
        $this->assertNotContains('enable_l2_for_wrappers', $args);
    }

    public function testEnableL2ForWrappersFalseyValuesTreatedAsFalse(): void
    {
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', ['enable_l2_for_wrappers' => $falsey]);
            $this->assertFalse(
                $this->enableL2ForWrappersFieldValue($gl),
                'enable_l2_for_wrappers => ' . var_export($falsey, true) . ' should be false'
            );
        }
    }

    public function testEnableL2ForWrappersFlagAppearsInSpawnedArgv(): void
    {
        // End-to-end assertion: start() with enable_l2_for_wrappers => true
        // must pass --enable-l2-for-wrappers to the spawned binary. Use a
        // fake binary that records its argv to a tempfile and listens on the
        // requested port long enough for waitForPort() to succeed.
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }

        $port = $this->findFreePort();
        $argvFile = tempnam(sys_get_temp_dir(), 'gl_argv_');
        register_shutdown_function(fn() => @unlink($argvFile));

        // Shell records its own argv (one arg per line) to $argvFile, then
        // python3 binds the port and idles long enough for the test to
        // observe it.
        $script = "#!/bin/sh\n"
            . "for arg in \"\$@\"; do\n"
            . "  printf '%s\\n' \"\$arg\" >> '{$argvFile}'\n"
            . "done\n"
            . "exec python3 -c \"import socket,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\"\n";

        $fake = tempnam(sys_get_temp_dir(), 'gl_fake_');
        file_put_contents($fake, $script);
        chmod($fake, 0755);
        register_shutdown_function(fn() => @unlink($fake));

        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");
            GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                [
                    'proxy_port' => $port,
                    'dashboard_port' => 0,
                    'enable_l2_for_wrappers' => true,
                ]
            );

            $argv = file_get_contents($argvFile);
            $this->assertIsString($argv);
            $this->assertStringContainsString(
                "--enable-l2-for-wrappers\n",
                $argv,
                "spawned argv must contain --enable-l2-for-wrappers when the option is true; got: {$argv}"
            );
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    public function testEnableL2ForWrappersDefaultOmitsFlagFromSpawnedArgv(): void
    {
        // Negative case: when the option is not set (default false), the
        // flag must NOT appear in argv. Guards against a regression that
        // would force the flag on for everyone.
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }

        $port = $this->findFreePort();
        $argvFile = tempnam(sys_get_temp_dir(), 'gl_argv_');
        register_shutdown_function(fn() => @unlink($argvFile));

        $script = "#!/bin/sh\n"
            . "for arg in \"\$@\"; do\n"
            . "  printf '%s\\n' \"\$arg\" >> '{$argvFile}'\n"
            . "done\n"
            . "exec python3 -c \"import socket,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\"\n";

        $fake = tempnam(sys_get_temp_dir(), 'gl_fake_');
        file_put_contents($fake, $script);
        chmod($fake, 0755);
        register_shutdown_function(fn() => @unlink($fake));

        $origBinary = getenv('GOLDLAPEL_BINARY');

        try {
            putenv("GOLDLAPEL_BINARY={$fake}");
            GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                ['proxy_port' => $port, 'dashboard_port' => 0]
            );

            $argv = (string) file_get_contents($argvFile);
            $this->assertStringNotContainsString(
                '--enable-l2-for-wrappers',
                $argv,
                "spawned argv must not contain --enable-l2-for-wrappers by default; got: {$argv}"
            );
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    private function findFreePort(): int
    {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        $name = stream_socket_get_name($server, false);
        $port = (int) explode(':', $name)[1];
        fclose($server);
        return $port;
    }
}
