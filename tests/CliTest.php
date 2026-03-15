<?php

namespace GoldLapel\Tests;

use PHPUnit\Framework\TestCase;

class CliTest extends TestCase
{
    private static string $binPath;

    public static function setUpBeforeClass(): void
    {
        self::$binPath = realpath(__DIR__ . '/../bin/goldlapel');
    }

    private function runCli(array $args = [], array $env = []): array
    {
        $cmd = [PHP_BINARY, self::$binPath, ...$args];

        $envVars = $_ENV;
        foreach ($env as $k => $v) {
            $envVars[$k] = $v;
        }

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($cmd, $descriptors, $pipes, null, $envVars);
        $this->assertIsResource($process);

        fclose($pipes[0]);
        $stdout = stream_get_contents($pipes[1]);
        fclose($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);

        return ['stdout' => $stdout, 'stderr' => $stderr, 'exitCode' => $exitCode];
    }

    public function testForwardsArgsToBinary(): void
    {
        $script = tempnam(sys_get_temp_dir(), 'gl_cli_');
        file_put_contents($script, "#!/bin/sh\necho \"\$@\"\n");
        chmod($script, 0755);

        try {
            $result = $this->runCli(['arg1', 'arg2', '--flag'], [
                'GOLDLAPEL_BINARY' => $script,
            ]);

            $this->assertSame(0, $result['exitCode']);
            $this->assertSame("arg1 arg2 --flag\n", $result['stdout']);
        } finally {
            unlink($script);
        }
    }

    public function testPropagatesExitCode(): void
    {
        $script = tempnam(sys_get_temp_dir(), 'gl_cli_');
        file_put_contents($script, "#!/bin/sh\nexit 42\n");
        chmod($script, 0755);

        try {
            $result = $this->runCli([], [
                'GOLDLAPEL_BINARY' => $script,
            ]);

            $this->assertSame(42, $result['exitCode']);
        } finally {
            unlink($script);
        }
    }

    public function testErrorWhenBinaryNotFound(): void
    {
        $result = $this->runCli([], [
            'GOLDLAPEL_BINARY' => '/nonexistent/goldlapel',
            'PATH' => '',
        ]);

        $this->assertSame(1, $result['exitCode']);
        $this->assertStringContainsString('Error:', $result['stderr']);
        $this->assertStringContainsString('not found', $result['stderr']);
    }

    public function testActivateCommandForwarded(): void
    {
        $script = tempnam(sys_get_temp_dir(), 'gl_cli_');
        file_put_contents($script, "#!/bin/sh\necho \"\$@\"\n");
        chmod($script, 0755);

        try {
            $result = $this->runCli(['activate', 'test-token-123'], [
                'GOLDLAPEL_BINARY' => $script,
            ]);

            $this->assertSame(0, $result['exitCode']);
            $this->assertSame("activate test-token-123\n", $result['stdout']);
        } finally {
            unlink($script);
        }
    }

    public function testNoArgsForwardsCleanly(): void
    {
        $script = tempnam(sys_get_temp_dir(), 'gl_cli_');
        file_put_contents($script, "#!/bin/sh\necho \"argc=\$#\"\n");
        chmod($script, 0755);

        try {
            $result = $this->runCli([], [
                'GOLDLAPEL_BINARY' => $script,
            ]);

            $this->assertSame(0, $result['exitCode']);
            $this->assertSame("argc=0\n", $result['stdout']);
        } finally {
            unlink($script);
        }
    }
}
