<?php

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for the Amp factory's start()-catch cleanup invariants.
 *
 * Two HIGH-severity parity gaps (with the sync factory) that were flagged
 * in the v0.2 burndown follow-up review:
 *
 *   1. Inner-guarded cleanup: if $instance->terminate() itself throws
 *      during the catch block (rare — a proc_close() edge case, SIGTERM
 *      race), the original connect exception must NOT be masked by the
 *      teardown failure. The user cares about the connect failure, not
 *      the cleanup failure.
 *
 *   2. Register-after-banner order: printBanner() now runs BEFORE
 *      registerForCleanup(), so an fwrite() throw (broken stderr pipe in
 *      a long-running SAPI, userland stream wrapper rejecting the write)
 *      cannot leak an orphan reference into $liveInstances.
 *
 * Both bugs are the same class the sync fix (`4282ad4`) closed on the
 * sync side; this test locks the Amp side to the same contract.
 */
class StartCleanupGuardTest extends TestCase
{
    // ------------------------------------------------------------------
    // Fix 1 — terminate-throws-during-catch does not mask original exception
    // ------------------------------------------------------------------

    public function testTerminateThrowingInCatchDoesNotMaskOriginalConnectException(): void
    {
        if (!class_exists(\Amp\Postgres\PostgresConfig::class)) {
            $this->markTestSkipped('amphp/postgres not installed');
        }
        if (!function_exists('posix_kill')) {
            $this->markTestSkipped('posix_kill unavailable');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH');
        }

        $port = $this->findFreePort();
        $pidFile = tempnam(sys_get_temp_dir(), 'gl_amp_mask_pid_');
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

        try {
            $caught = null;
            try {
                // AmpGoldLapelTerminateThrows overrides terminate() to throw
                // a distinctive exception. If the fix is missing, that
                // exception replaces the real connect failure in the
                // propagation chain. If the fix is present, the original
                // connect failure propagates unchanged and the terminate
                // exception is swallowed.
                AmpGoldLapelTerminateThrows::start(
                    'postgresql://user:pass@localhost:5432/testdb',
                    ['port' => $port, 'dashboard_port' => 0, 'silent' => true]
                )->await();
                $this->fail('start()->await() should have thrown.');
            } catch (\Throwable $e) {
                $caught = $e;
            }

            $this->assertNotNull($caught, 'start() must throw on connect failure.');
            $this->assertNotInstanceOf(
                TerminateSabotageException::class,
                $caught,
                'Original connect exception was masked by terminate() teardown error. '
                . "start() saw: " . $caught::class . ': ' . $caught->getMessage()
            );

            // The original connect exception is propagated unchanged — neither
            // wrapped via ->getPrevious() nor replaced. Check the previous
            // chain too, defensively: the sabotage exception must NEVER appear
            // anywhere in the chain, because we SWALLOW it (don't chain it).
            $cursor = $caught;
            while ($cursor !== null) {
                $this->assertNotInstanceOf(
                    TerminateSabotageException::class,
                    $cursor,
                    'Terminate-failure exception must be swallowed, never chained. '
                    . 'Chaining still surfaces it to the user (via getPrevious) and '
                    . 'is semantically different from the sync factory\'s contract.'
                );
                $cursor = $cursor->getPrevious();
            }

            // And liveInstances must be empty — unset() runs after the inner
            // try/catch regardless of whether terminate() threw.
            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            $this->assertCount(
                0,
                $ref->getValue(),
                '$liveInstances must be empty after start() throws, even when terminate() throws.'
            );
        } finally {
            GoldLapel::cleanupAll();
            // We override terminate() in the subclass, so the real
            // subprocess is still alive. Kill it manually via the PID file
            // so we don't leak across tests.
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
    // Fix 2 — banner throw before register cannot leak liveInstances
    // ------------------------------------------------------------------

    public function testBannerWriteFailureBeforeRegistrationDoesNotLeak(): void
    {
        if (!function_exists('posix_kill')) {
            $this->markTestSkipped('posix_kill unavailable');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH');
        }

        // Accept-and-close fake: lets waitForPort() succeed, so we reach the
        // banner + register code path (what we're testing), without needing
        // amphp/postgres to make connect throw later.
        $port = $this->findFreePort();
        $pidFile = tempnam(sys_get_temp_dir(), 'gl_amp_banner_pid_');
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

        // Register a stream wrapper that throws on stream_write. Swap the
        // Amp factory's static $bannerStream to a handle over our wrapper.
        ThrowingStreamWrapper::register();
        $throwingStream = fopen('throwing-banner://test', 'w');

        $bannerRef = new \ReflectionProperty(GoldLapel::class, 'bannerStream');
        $bannerRef->setAccessible(true);
        $originalBannerStream = $bannerRef->getValue();
        $bannerRef->setValue(null, $throwingStream);

        try {
            $caught = null;
            try {
                GoldLapel::start(
                    'postgresql://user:pass@localhost:5432/testdb',
                    ['port' => $port, 'dashboard_port' => 0, 'silent' => false]
                )->await();
                $this->fail('start()->await() should have thrown from banner write.');
            } catch (\Throwable $e) {
                $caught = $e;
            }

            $this->assertNotNull($caught, 'start() must throw when banner write fails.');

            // The critical invariant: the instance was NOT registered in
            // $liveInstances before printBanner() threw, so no cleanup is
            // needed and no orphan reference remains.
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
            ThrowingStreamWrapper::unregister();
            GoldLapel::cleanupAll();
            // Kill the leftover fake (start() threw before we got a handle
            // back, so no stop() is possible — clean up via pidfile).
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
