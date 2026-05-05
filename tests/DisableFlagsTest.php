<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the four "master kill switch" disable flags promoted from the
 * structured config map to top-level options:
 *
 *   - disable_proxy_cache    → --disable-proxy-cache
 *   - disable_matviews       → --disable-matviews
 *   - disable_sqloptimize    → --disable-sqloptimize
 *   - disable_auto_indexes   → --disable-auto-indexes
 *
 * Each maps 1:1 to a proxy CLI flag at spawn time. Atomic break: the keys
 * are no longer accepted inside `config[...]` (passing them there raises
 * `Unknown config key`).
 */
class DisableFlagsTest extends TestCase
{
    private function fieldValue(GoldLapel $gl, string $field): bool
    {
        $ref = new \ReflectionProperty(GoldLapel::class, $field);
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    /**
     * Test data: option-key → reflection-property → CLI flag.
     *
     * @return list<array{0: string, 1: string, 2: string}>
     */
    public static function flagMatrix(): array
    {
        return [
            ['disable_proxy_cache', 'disableProxyCache', '--disable-proxy-cache'],
            ['disable_matviews', 'disableMatviews', '--disable-matviews'],
            ['disable_sqloptimize', 'disableSqloptimize', '--disable-sqloptimize'],
            ['disable_auto_indexes', 'disableAutoIndexes', '--disable-auto-indexes'],
        ];
    }

    // ─── default + parsing ─────────────────────────────────────────────

    #[DataProvider('flagMatrix')]
    public function testDefaultsToFalse(string $option, string $field, string $cliFlag): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->fieldValue($gl, $field));
    }

    #[DataProvider('flagMatrix')]
    public function testOptionParsedAsTrue(string $option, string $field, string $cliFlag): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [$option => true]);
        $this->assertTrue($this->fieldValue($gl, $field));
    }

    #[DataProvider('flagMatrix')]
    public function testFalseyValuesTreatedAsFalse(string $option, string $field, string $cliFlag): void
    {
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', [$option => $falsey]);
            $this->assertFalse(
                $this->fieldValue($gl, $field),
                "{$option} => " . var_export($falsey, true) . ' should be false',
            );
        }
    }

    // ─── atomic break: not valid inside structured config ──────────────

    #[DataProvider('flagMatrix')]
    public function testAsConfigKeyRejected(string $option, string $field, string $cliFlag): void
    {
        // Belt-and-braces: each promoted flag is a top-level
        // canonical-surface option, never valid inside the structured
        // config map.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("Unknown config key: {$option}");
        new GoldLapel('postgresql://u:p@h/d', [
            'config' => [$option => true],
        ]);
    }

    // ─── argv emission: 1:1 mapping to proxy CLI flag ──────────────────

    #[DataProvider('flagMatrix')]
    public function testFlagAppearsInSpawnedArgv(string $option, string $field, string $cliFlag): void
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }
        if (!is_executable('/usr/bin/python3') && !is_executable('/usr/local/bin/python3')) {
            $this->markTestSkipped('python3 required for fake binary that holds the port open');
        }

        [$port, $argvFile, $cleanup] = $this->spawnFakeBinaryAndStart([$option => true]);
        try {
            $argv = (string) file_get_contents($argvFile);
            $this->assertStringContainsString(
                $cliFlag . "\n",
                $argv,
                "spawned argv must contain {$cliFlag} when {$option} is true; got: {$argv}",
            );
        } finally {
            $cleanup();
        }
    }

    public function testDefaultArgvOmitsAllFourFlags(): void
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }
        if (!is_executable('/usr/bin/python3') && !is_executable('/usr/local/bin/python3')) {
            $this->markTestSkipped('python3 required for fake binary that holds the port open');
        }

        [$port, $argvFile, $cleanup] = $this->spawnFakeBinaryAndStart([]);
        try {
            $argv = (string) file_get_contents($argvFile);
            foreach (self::flagMatrix() as [$option, $field, $cliFlag]) {
                $this->assertStringNotContainsString(
                    $cliFlag,
                    $argv,
                    "default argv must not contain {$cliFlag}; got: {$argv}",
                );
            }
        } finally {
            $cleanup();
        }
    }

    public function testMultipleFlagsAllAppearTogether(): void
    {
        // Combined startup: turn all four on at once. Verifies each flag
        // is independently emitted and ordering doesn't trip the
        // argv-builder up.
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }
        if (!is_executable('/usr/bin/python3') && !is_executable('/usr/local/bin/python3')) {
            $this->markTestSkipped('python3 required for fake binary that holds the port open');
        }

        $opts = [
            'disable_proxy_cache' => true,
            'disable_matviews' => true,
            'disable_sqloptimize' => true,
            'disable_auto_indexes' => true,
        ];
        [$port, $argvFile, $cleanup] = $this->spawnFakeBinaryAndStart($opts);
        try {
            $argv = (string) file_get_contents($argvFile);
            foreach (self::flagMatrix() as [$option, $field, $cliFlag]) {
                $this->assertStringContainsString(
                    $cliFlag . "\n",
                    $argv,
                    "combined argv must contain {$cliFlag}; got: {$argv}",
                );
            }
        } finally {
            $cleanup();
        }
    }

    // ─── helpers ───────────────────────────────────────────────────────

    /**
     * Spawn a fake binary that records its argv to a tempfile and binds
     * the requested port long enough for waitForPort() to succeed.
     *
     * @param array<string, mixed> $extraOptions
     * @return array{0: int, 1: string, 2: callable}
     */
    private function spawnFakeBinaryAndStart(array $extraOptions): array
    {
        $port = $this->findFreePort();
        $argvFile = tempnam(sys_get_temp_dir(), 'gl_argv_');
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

        $origBinary = getenv('GOLDLAPEL_BINARY');
        putenv("GOLDLAPEL_BINARY={$fake}");

        $opts = array_merge(
            ['proxy_port' => $port, 'dashboard_port' => 0],
            $extraOptions,
        );
        GoldLapel::startProxyOnly('postgresql://user:pass@localhost:5432/db', $opts);

        $cleanup = function () use ($origBinary, $argvFile, $fake): void {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
            @unlink($argvFile);
            @unlink($fake);
        };

        return [$port, $argvFile, $cleanup];
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
