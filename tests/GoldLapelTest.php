<?php

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class GoldLapelTest extends TestCase
{
    // -- FindBinary (3 tests) --

    private string|false $originalGoldlapelBinary;

    protected function setUp(): void
    {
        $this->originalGoldlapelBinary = getenv('GOLDLAPEL_BINARY');
    }

    protected function tearDown(): void
    {
        if ($this->originalGoldlapelBinary === false) {
            putenv('GOLDLAPEL_BINARY');
        } else {
            putenv("GOLDLAPEL_BINARY={$this->originalGoldlapelBinary}");
        }
    }

    public function testFindBinaryEnvVarOverride(): void
    {
        $tmp = tempnam(sys_get_temp_dir(), 'gl_');
        try {
            putenv("GOLDLAPEL_BINARY={$tmp}");
            $this->assertSame($tmp, GoldLapel::findBinary());
        } finally {
            unlink($tmp);
        }
    }

    public function testFindBinaryEnvVarMissingFile(): void
    {
        putenv('GOLDLAPEL_BINARY=/nonexistent/goldlapel');
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/GOLDLAPEL_BINARY.*not found/');
        GoldLapel::findBinary();
    }

    public function testFindBinaryNotFoundRaises(): void
    {
        putenv('GOLDLAPEL_BINARY');
        $origPath = getenv('PATH');
        try {
            putenv('PATH=');
            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessageMatches('/Gold Lapel binary not found/');
            GoldLapel::findBinary();
        } finally {
            putenv("PATH={$origPath}");
        }
    }

    // -- MakeProxyUrl (14 tests) --

    public function testMakeProxyUrlPostgresqlFull(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:pass@dbhost:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlPostgresScheme(): void
    {
        $this->assertSame(
            'postgres://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgres://user:pass@remote.aws.com:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoPort(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:pass@host.aws.com/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoPortNoPath(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932',
            GoldLapel::makeProxyUrl('postgresql://user:pass@host.aws.com', 7932)
        );
    }

    public function testMakeProxyUrlBareHostPort(): void
    {
        $this->assertSame(
            'localhost:7932',
            GoldLapel::makeProxyUrl('dbhost:5432', 7932)
        );
    }

    public function testMakeProxyUrlBareHost(): void
    {
        $this->assertSame(
            'localhost:7932',
            GoldLapel::makeProxyUrl('dbhost', 7932)
        );
    }

    public function testMakeProxyUrlQueryParams(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb?sslmode=require',
            GoldLapel::makeProxyUrl('postgresql://user:pass@remote:5432/mydb?sslmode=require', 7932)
        );
    }

    public function testMakeProxyUrlPercentEncoded(): void
    {
        $this->assertSame(
            'postgresql://user:p%40ss@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:p%40ss@remote:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoUserWithPort(): void
    {
        $this->assertSame(
            'postgresql://localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://dbhost:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlNoUserNoPort(): void
    {
        $this->assertSame(
            'postgresql://localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://dbhost/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLocalhostUpstream(): void
    {
        $this->assertSame(
            'postgresql://user:pass@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:pass@localhost:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLiteralAtInPassword(): void
    {
        $this->assertSame(
            'postgresql://user:p@ss@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:p@ss@host:5432/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLiteralAtInPasswordNoPort(): void
    {
        $this->assertSame(
            'postgresql://user:p@ss@localhost:7932/mydb',
            GoldLapel::makeProxyUrl('postgresql://user:p@ss@host/mydb', 7932)
        );
    }

    public function testMakeProxyUrlLiteralAtInPasswordWithQueryParams(): void
    {
        $this->assertSame(
            'postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue',
            GoldLapel::makeProxyUrl('postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue', 7932)
        );
    }

    // -- WaitForPort (2 tests) --

    public function testWaitForPortOpenPortReturnsTrue(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);

        $name = stream_socket_get_name($server, false);
        $port = (int) explode(':', $name)[1];

        try {
            $this->assertTrue(GoldLapel::waitForPort('127.0.0.1', $port, 2.0));
        } finally {
            fclose($server);
        }
    }

    public function testWaitForPortClosedPortTimesOut(): void
    {
        $this->assertFalse(GoldLapel::waitForPort('127.0.0.1', 19999, 0.2));
    }

    // -- GoldLapel class (5 tests) --

    public function testDefaultPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertSame(7932, $gl->getPort());
    }

    public function testCustomPort(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db', 9000);
        $this->assertSame(9000, $gl->getPort());
    }

    public function testNotRunningInitially(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $this->assertFalse($gl->isRunning());
    }

    public function testStopNoOp(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $gl->stopProxy();
        $this->assertFalse($gl->isRunning());
    }

    public function testStopIdempotent(): void
    {
        $gl = new GoldLapel('postgresql://host:5432/db');
        $gl->stopProxy();
        $gl->stopProxy();
        $this->assertFalse($gl->isRunning());
    }

    // -- Static singleton (1 test) --

    public function testProxyUrlNullWhenNotStarted(): void
    {
        GoldLapel::stop();
        $this->assertNull(GoldLapel::proxyUrl());
    }
}
