<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;

/**
 * Regression test: GoldLapel::start() must clean up its subprocess if the
 * eager PDO construction raises after the subprocess has been spawned.
 *
 * This mirrors the Python (test_v02_subprocess_cleanup.py) and Ruby
 * (test_v02_subprocess_cleanup.rb) equivalents — same bug class, bitten
 * twice before in sibling wrappers. The invariant: when the subprocess
 * spawns OK but PDO construction fails, the subprocess MUST be terminated
 * at exception-propagation time, not deferred to the shutdown hook. A
 * long-running PHP process (Octane, Swoole, RoadRunner, a CLI daemon)
 * that retries start() against a bad upstream would otherwise accumulate
 * orphan goldlapel children for the lifetime of the worker.
 *
 * Strategy: swap in a fake "goldlapel" binary via GOLDLAPEL_BINARY that
 * binds the proxy port (so startProxyWithoutConnect() succeeds) but
 * slams every incoming TCP connection (so PDO construction explodes).
 * The fake writes its PID to a tempfile so we can assert death via
 * `posix_kill($pid, 0)` AFTER start() throws — without manually
 * invoking cleanupAll. If the sync wrapper ever regresses to "defer
 * cleanup to shutdown hook", this test fails at the posix_kill check.
 */
class SubprocessCleanupTest extends TestCase
{
    public function testSubprocessCleanupOnConnectFailure(): void
    {
        if (!extension_loaded('pdo_pgsql')) {
            $this->markTestSkipped('pdo_pgsql not loaded');
        }
        if (!function_exists('posix_kill')) {
            $this->markTestSkipped('posix_kill unavailable — required to check PID liveness');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH — required for fake binary');
        }

        $port = $this->findFreePort();
        $pidFile = tempnam(sys_get_temp_dir(), 'gl_sub_cleanup_pid_');
        register_shutdown_function(fn () => @unlink($pidFile));

        // Fake binary: bind port, record PID, accept-and-close forever. PDO
        // construction against a listener that doesn't speak Postgres throws.
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
                    ['proxy_port' => $port, 'dashboard_port' => 0, 'silent' => true]
                );
                $this->fail('start() should have thrown — fake listener does not speak Postgres.');
            } catch (\PDOException $e) {
                $threw = true;
            } catch (\RuntimeException $e) {
                // Some environments bubble the failure as RuntimeException.
                $threw = true;
            }
            $this->assertTrue($threw, 'start() must throw on PDO failure.');

            // Wait briefly for the fake binary to have written its PID —
            // proves the subprocess actually spawned before the PDO step.
            $pid = $this->awaitPid($pidFile);
            $this->assertNotNull($pid, 'Fake subprocess should have recorded its PID.');

            // Critical invariant: at exception-propagation time, the
            // subprocess must already be dead. We poll for a short grace
            // window (SIGTERM → wait → SIGKILL is not instantaneous) but we
            // do NOT call cleanupAll() ourselves — the wrapper's own
            // start()-catch path must do the work.
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

            // And $liveInstances must have been drained: the failing
            // start() should not leave an orphan reference behind.
            $ref = new \ReflectionProperty(GoldLapel::class, 'liveInstances');
            $ref->setAccessible(true);
            $this->assertCount(
                0,
                $ref->getValue(),
                '$liveInstances must be empty after start() throws — otherwise stop() was not called.'
            );
        } finally {
            // Defensive belt-and-braces: if the test itself fails partway,
            // make sure we don't leak a fake python process across tests.
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
        }
    }

    // ------------------------------------------------------------------
    // helpers (kept local — parity with Python/Ruby's standalone tests)
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
