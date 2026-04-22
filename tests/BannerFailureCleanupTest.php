<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Regression test: a banner-write failure during start() must NOT leak
 * an orphan entry into $liveInstances.
 *
 * Fix (sync + Amp): printBanner() is now called BEFORE
 * registerForCleanup(), so fwrite()'s (very rare) throw on a broken
 * stderr pipe — closed fd in a long-running SAPI, unwritable stream,
 * FPM after fastcgi_finish_request() detaches stderr — propagates
 * cleanly without a registry entry that would otherwise orphan the
 * subprocess reference.
 *
 * On the sync path, the outer try/catch added in commit 4282ad4
 * "accidentally" closes this window too (the banner throw is caught
 * alongside the PDO throw). This test still locks the invariant in
 * place at the source-of-truth: the register-after-banner ordering.
 */
class BannerFailureCleanupTest extends TestCase
{
    public function testBannerThrowBeforeRegistrationDoesNotLeak(): void
    {
        if (!function_exists('posix_kill')) {
            $this->markTestSkipped('posix_kill unavailable');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH');
        }

        // Accept-and-close fake: waitForPort() returns true, we enter the
        // banner-then-register section, banner throws. startProxy()'s outer
        // catch runs stop(), which is idempotent on an unregistered
        // instance — so we expect both: the exception propagates, AND the
        // liveInstances registry is empty.
        $port = $this->findFreePort();
        $pidFile = tempnam(sys_get_temp_dir(), 'gl_banner_fail_pid_');
        register_shutdown_function(fn () => @unlink($pidFile));

        $fake = $this->makeFakeBinary(
            "#!/bin/sh\n"
            . "exec python3 -c \"import socket,os\n"
            . "open('{$pidFile}','w').write(str(os.getpid()))\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "while True:\n"
            . "    c,_=s.accept()\n"
            . "    c.close()\"\n"
        );

        $origBinary = getenv('GOLDLAPEL_BINARY');
        putenv("GOLDLAPEL_BINARY={$fake}");

        \GoldLapel\Amp\Tests\ThrowingStreamWrapper::register();
        $throwingStream = fopen('throwing-banner://test', 'w');

        $bannerRef = new \ReflectionProperty(GoldLapel::class, 'bannerStream');
        $bannerRef->setAccessible(true);
        $originalBannerStream = $bannerRef->getValue();
        $bannerRef->setValue(null, $throwingStream);

        try {
            $caught = null;
            try {
                GoldLapel::startProxyOnly(
                    'postgresql://user:pass@localhost:5432/testdb',
                    ['proxy_port' => $port, 'dashboard_port' => 0, 'silent' => false]
                );
                $this->fail('startProxyOnly() should have thrown from banner write.');
            } catch (\Throwable $e) {
                $caught = $e;
            }

            $this->assertNotNull($caught, 'startProxyOnly() must throw when banner write fails.');

            // The critical invariant: instance was never registered, so
            // $liveInstances remains empty.
            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            $this->assertCount(
                0,
                $ref->getValue(),
                '$liveInstances must be empty: a banner throw before '
                . 'registerForCleanup() must not leak a registry entry.'
            );
        } finally {
            $bannerRef->setValue(null, $originalBannerStream);
            if (is_resource($throwingStream)) {
                fclose($throwingStream);
            }
            \GoldLapel\Amp\Tests\ThrowingStreamWrapper::unregister();
            GoldLapel::cleanupAll();
            $pid = @file_get_contents($pidFile);
            if ($pid !== false && $pid !== '') {
                @posix_kill((int) trim($pid), 9);
            }
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private function makeFakeBinary(string $contents): string
    {
        $path = tempnam(sys_get_temp_dir(), 'gl_fake_');
        file_put_contents($path, $contents);
        chmod($path, 0755);
        register_shutdown_function(fn () => @unlink($path));
        return $path;
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
