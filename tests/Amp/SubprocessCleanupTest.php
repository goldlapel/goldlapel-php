<?php

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Regression test: GoldLapel\Amp\GoldLapel::start() must clean up its
 * subprocess if the async Postgres connect raises after the subprocess
 * has been spawned.
 *
 * Mirrors the sync tests/SubprocessCleanupTest.php, the Python
 * test_v02_subprocess_cleanup.py, and the Ruby test_v02_subprocess_cleanup.rb.
 * Same bug class, bitten twice before in sibling wrappers. Invariant:
 * when the subprocess spawns OK but the async connect step fails, the
 * subprocess MUST be terminated at exception-propagation time (inside
 * start()'s catch), not left to the shutdown hook. A long-running
 * event-loop-driven PHP worker that retries start() against a bad
 * upstream would otherwise accumulate orphan goldlapel children for
 * the lifetime of the process.
 *
 * Strategy: swap in a fake "goldlapel" binary via GOLDLAPEL_BINARY that
 * binds the proxy port but slams incoming TCP (so amphp/postgres's
 * handshake fails). The fake writes its PID to a tempfile so we can
 * assert it's dead via `posix_kill($pid, 0)` AFTER start() throws —
 * without manually invoking cleanupAll. If the Amp wrapper ever
 * regresses to "defer cleanup to shutdown hook", this test fails at
 * the posix_kill check.
 */
class SubprocessCleanupTest extends TestCase
{
    public function testSubprocessCleanupOnConnectFailure(): void
    {
        if (!class_exists(\Amp\Postgres\PostgresConfig::class)) {
            $this->markTestSkipped('amphp/postgres not installed');
        }
        if (!function_exists('posix_kill')) {
            $this->markTestSkipped('posix_kill unavailable — required to check PID liveness');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH — required for fake binary');
        }

        $port = $this->findFreePort();
        $pidFile = tempnam(sys_get_temp_dir(), 'gl_amp_sub_cleanup_pid_');
        register_shutdown_function(fn () => @unlink($pidFile));

        // Fake binary: bind port, record PID, accept-and-close forever.
        // amphp's Postgres connect sends a startup packet and expects a
        // handshake response — the abrupt close causes connect to throw.
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
            $threw = false;
            try {
                GoldLapel::start(
                    'postgresql://user:pass@localhost:5432/testdb',
                    ['port' => $port, 'dashboard_port' => 0, 'silent' => true]
                )->await();
                $this->fail('start()->await() should have thrown — fake listener rejects Postgres handshake.');
            } catch (\Throwable $e) {
                // amphp/postgres raises a variety of connection-failure types
                // depending on the driver (pq vs pgsql) and exactly where in
                // the handshake the abrupt close lands — we don't pin to a
                // specific exception class, only "some throw propagated".
                $threw = true;
            }
            $this->assertTrue($threw, 'start()->await() must throw on async connect failure.');

            // The fake binary wrote its PID to $pidFile the moment it
            // spawned — proves the subprocess actually started before the
            // connect step failed.
            $pid = $this->awaitPid($pidFile);
            $this->assertNotNull($pid, 'Fake subprocess should have recorded its PID.');

            // Invariant: at exception-propagation time, the subprocess must
            // already be dead. Poll for a short grace window (SIGTERM →
            // wait → SIGKILL is not instantaneous). We do NOT call
            // cleanupAll() ourselves — the wrapper's own start()-catch
            // path must do the work.
            $deadline = hrtime(true) + (int) (5 * 1e9);
            $alive = true;
            while (hrtime(true) < $deadline) {
                $alive = @posix_kill($pid, 0);
                if (!$alive) {
                    break;
                }
                usleep(50000);
            }
            $this->assertFalse(
                $alive,
                "Subprocess PID {$pid} must be terminated when start() throws — "
                . 'not deferred to the shutdown hook. cleanupAll was NOT called in this test.'
            );

            // $liveInstances must have been drained: a failing start()
            // should not leave an orphan registry entry behind.
            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            $this->assertCount(
                0,
                $ref->getValue(),
                '$liveInstances must be empty after start() throws — otherwise terminate() was not called.'
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

    // ------------------------------------------------------------------
    // helpers (kept local — parity with the sync SubprocessCleanupTest)
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

    private function awaitPid(string $pidFile, float $timeoutSec = 2.0): ?int
    {
        $deadline = hrtime(true) + (int) ($timeoutSec * 1e9);
        while (hrtime(true) < $deadline) {
            $contents = @file_get_contents($pidFile);
            if ($contents !== false && $contents !== '') {
                $pid = (int) trim($contents);
                if ($pid > 0) {
                    return $pid;
                }
            }
            usleep(50000);
        }
        return null;
    }
}
