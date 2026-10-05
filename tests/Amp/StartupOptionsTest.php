<?php

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\GoldLapel;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Amp mirror of tests/DisableFlagsTest.php and the mesh tests in
 * tests/BannerTest.php. The async factory used to drop `mesh`, `mesh_tag`
 * and the three `disable_*` kill switches on the floor, and only validated
 * `config` keys at spawn time — these pin it to the sync factory's surface.
 */
class StartupOptionsTest extends TestCase
{
    private function fieldValue(GoldLapel $gl, string $field): mixed
    {
        $ref = new \ReflectionProperty(GoldLapel::class, $field);
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    /**
     * @return list<array{0: string, 1: string, 2: string}>
     */
    public static function flagMatrix(): array
    {
        return [
            ['disable_proxy_cache', 'disableProxyCache', '--disable-proxy-cache'],
            ['disable_sqloptimize', 'disableSqloptimize', '--disable-sqloptimize'],
            ['disable_auto_indexes', 'disableAutoIndexes', '--disable-auto-indexes'],
        ];
    }

    // ─── disable flags: parsing ────────────────────────────────────────

    #[DataProvider('flagMatrix')]
    public function testDisableFlagDefaultsToFalse(string $option, string $field, string $cliFlag): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->fieldValue($gl, $field));
    }

    #[DataProvider('flagMatrix')]
    public function testDisableFlagParsedAsTrue(string $option, string $field, string $cliFlag): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', [$option => true]);
        $this->assertTrue($this->fieldValue($gl, $field));
    }

    #[DataProvider('flagMatrix')]
    public function testDisableFlagFalseyValuesTreatedAsFalse(string $option, string $field, string $cliFlag): void
    {
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', [$option => $falsey]);
            $this->assertFalse(
                $this->fieldValue($gl, $field),
                "{$option} => " . var_export($falsey, true) . ' should be false',
            );
        }
    }

    #[DataProvider('flagMatrix')]
    public function testDisableFlagAsConfigKeyRejectedAtConstruction(string $option, string $field, string $cliFlag): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("Unknown config key: {$option}");
        new GoldLapel('postgresql://u:p@h/d', ['config' => [$option => true]]);
    }

    public function testUnknownTopLevelOptionRejectedAtConstruction(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown option: invalidation_port (it was removed with the in-process cache)');
        new GoldLapel('postgresql://u:p@h/d', ['invalidation_port' => 7934]);
    }

    public function testBadLogLevelRejectedAtConstruction(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', ['log_level' => 'loud']);
    }

    public function testUnknownConfigKeyRejectedAtConstruction(): void
    {
        // Eager validation — matches the sync constructor, so a typo fails
        // before any subprocess is spawned.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown config key: pool_sise');
        new GoldLapel('postgresql://u:p@h/d', ['config' => ['pool_sise' => 5]]);
    }

    // ─── mesh: parsing ─────────────────────────────────────────────────

    public function testMeshDefaultsToFalse(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d');
        $this->assertFalse($this->fieldValue($gl, 'mesh'));
        $this->assertNull($this->fieldValue($gl, 'meshTag'));
    }

    public function testMeshOptionParsedAsTrue(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['mesh' => true, 'mesh_tag' => 'prod-east']);
        $this->assertTrue($this->fieldValue($gl, 'mesh'));
        $this->assertSame('prod-east', $this->fieldValue($gl, 'meshTag'));
    }

    public function testMeshTagEmptyStringNormalizedToNull(): void
    {
        $gl = new GoldLapel('postgresql://u:p@h/d', ['mesh' => true, 'mesh_tag' => '']);
        $this->assertNull($this->fieldValue($gl, 'meshTag'));
    }

    public function testMeshFalseyValuesTreatedAsFalse(): void
    {
        foreach ([false, 0, '', null] as $falsey) {
            $gl = new GoldLapel('postgresql://u:p@h/d', ['mesh' => $falsey]);
            $this->assertFalse(
                $this->fieldValue($gl, 'mesh'),
                'mesh => ' . var_export($falsey, true) . ' should be false',
            );
        }
    }

    public function testMeshAsConfigKeyRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', ['config' => ['mesh' => true]]);
    }

    public function testMeshTagAsConfigKeyRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new GoldLapel('postgresql://u:p@h/d', ['config' => ['mesh_tag' => 'prod']]);
    }

    // ─── argv emission ─────────────────────────────────────────────────

    #[DataProvider('flagMatrix')]
    public function testDisableFlagAppearsInSpawnedArgv(string $option, string $field, string $cliFlag): void
    {
        $argv = $this->spawnAndCaptureArgv([$option => true]);
        $this->assertContains($cliFlag, $argv, "spawned argv must contain {$cliFlag} when {$option} is true");
    }

    public function testDefaultArgvOmitsOptionalFlags(): void
    {
        $argv = $this->spawnAndCaptureArgv([]);
        foreach (['--disable-proxy-cache', '--disable-sqloptimize', '--disable-auto-indexes', '--mesh', '--mesh-tag'] as $flag) {
            $this->assertNotContains($flag, $argv, "default argv must not contain {$flag}");
        }
    }

    public function testMeshAndTagAppearInSpawnedArgv(): void
    {
        $argv = $this->spawnAndCaptureArgv(['mesh' => true, 'mesh_tag' => 'prod-east']);
        $this->assertContains('--mesh', $argv);
        $i = array_search('--mesh-tag', $argv, true);
        $this->assertNotFalse($i, '--mesh-tag must be emitted when mesh_tag is set');
        $this->assertSame('prod-east', $argv[$i + 1]);
    }

    public function testEmptyMeshTagNotEmitted(): void
    {
        $argv = $this->spawnAndCaptureArgv(['mesh' => true, 'mesh_tag' => '']);
        $this->assertContains('--mesh', $argv);
        $this->assertNotContains('--mesh-tag', $argv);
    }

    public function testAllOptionsTogether(): void
    {
        $argv = $this->spawnAndCaptureArgv([
            'mesh' => true,
            'disable_proxy_cache' => true,
            'disable_sqloptimize' => true,
            'disable_auto_indexes' => true,
        ]);
        foreach (['--mesh', '--disable-proxy-cache', '--disable-sqloptimize', '--disable-auto-indexes'] as $flag) {
            $this->assertContains($flag, $argv);
        }
    }

    // ─── helpers ───────────────────────────────────────────────────────

    /**
     * Start the Amp factory against a fake binary that records its argv and
     * binds the requested port, then return the recorded argv.
     *
     * @param array<string, mixed> $extraOptions
     * @return list<string>
     */
    private function spawnAndCaptureArgv(array $extraOptions): array
    {
        if (PHP_OS_FAMILY === 'Windows') {
            $this->markTestSkipped('Fake-binary spawn test uses /bin/sh.');
        }
        if (trim((string) shell_exec('command -v python3 2>/dev/null')) === '') {
            $this->markTestSkipped('python3 not found on PATH — required for fake binary');
        }

        $port = $this->findFreePort();
        $argvFile = tempnam(sys_get_temp_dir(), 'gl_amp_argv_');
        $fake = tempnam(sys_get_temp_dir(), 'gl_amp_fake_');
        file_put_contents(
            $fake,
            "#!/bin/sh\n"
            . "for arg in \"\$@\"; do\n"
            . "  printf '%s\\n' \"\$arg\" >> '{$argvFile}'\n"
            . "done\n"
            . "exec python3 -c \"import socket,time\n"
            . "s=socket.socket()\n"
            . "s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
            . "s.bind(('127.0.0.1',{$port}))\n"
            . "s.listen(5)\n"
            . "time.sleep(30)\"\n"
        );
        chmod($fake, 0755);

        $origBinary = getenv('GOLDLAPEL_BINARY');
        putenv("GOLDLAPEL_BINARY={$fake}");
        try {
            GoldLapel::startProxyOnly(
                'postgresql://user:pass@localhost:5432/db',
                array_merge(['proxy_port' => $port, 'dashboard_port' => 0, 'silent' => true], $extraOptions),
            )->await();
            return explode("\n", rtrim((string) file_get_contents($argvFile), "\n"));
        } finally {
            GoldLapel::cleanupAll();
            if ($origBinary === false) {
                putenv('GOLDLAPEL_BINARY');
            } else {
                putenv("GOLDLAPEL_BINARY={$origBinary}");
            }
            @unlink($argvFile);
            @unlink($fake);
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
