<?php

namespace GoldLapel\Laravel\Tests;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase as PureTestCase;

use function GoldLapel\Laravel\buildUpstreamUrl;

class BuildUpstreamUrlTest extends PureTestCase
{
    public function testStandard(): void
    {
        $url = buildUpstreamUrl([
            'host' => 'db.example.com', 'port' => '5432', 'database' => 'mydb',
            'username' => 'admin', 'password' => 'secret',
        ]);
        $this->assertSame('postgresql://admin:secret@db.example.com:5432/mydb', $url);
    }

    public function testEmptyHostDefaultsToLocalhost(): void
    {
        $url = buildUpstreamUrl(['host' => '', 'port' => '5432', 'database' => 'db']);
        $this->assertStringContainsString('localhost:', $url);
    }

    public function testMissingHostDefaultsToLocalhost(): void
    {
        $url = buildUpstreamUrl(['port' => '5432', 'database' => 'db']);
        $this->assertStringContainsString('localhost:', $url);
    }

    public function testEmptyPortDefaultsTo5432(): void
    {
        $url = buildUpstreamUrl(['host' => 'h', 'port' => '', 'database' => 'db']);
        $this->assertStringContainsString(':5432/', $url);
    }

    public function testMissingPortDefaultsTo5432(): void
    {
        $url = buildUpstreamUrl(['host' => 'h', 'database' => 'db']);
        $this->assertStringContainsString(':5432/', $url);
    }

    public function testSpecialCharsInPassword(): void
    {
        $url = buildUpstreamUrl([
            'host' => 'h', 'port' => '5432', 'database' => 'db',
            'username' => 'u', 'password' => '@:/',
        ]);
        $this->assertStringContainsString('u:%40%3A%2F@', $url);
    }

    public function testSpecialCharsInUser(): void
    {
        $url = buildUpstreamUrl([
            'host' => 'h', 'port' => '5432', 'database' => 'db',
            'username' => 'u@ser', 'password' => 'p',
        ]);
        $this->assertStringContainsString('u%40ser:p@', $url);
    }

    public function testNoUserNoPassword(): void
    {
        $url = buildUpstreamUrl(['host' => 'h', 'port' => '5432', 'database' => 'db']);
        $this->assertSame('postgresql://h:5432/db', $url);
    }

    public function testUserWithoutPassword(): void
    {
        $url = buildUpstreamUrl([
            'host' => 'h', 'port' => '5432', 'database' => 'db',
            'username' => 'admin',
        ]);
        $this->assertStringContainsString('admin@h:', $url);
        $this->assertStringNotContainsString(':admin', $url);
    }

    public function testSpecialCharsInDatabase(): void
    {
        $url = buildUpstreamUrl([
            'host' => 'h', 'port' => '5432', 'database' => 'my#db?v=1',
            'username' => 'u', 'password' => 'p',
        ]);
        $this->assertStringEndsWith('/my%23db%3Fv%3D1', $url);
    }

    public function testUnixSocketRaises(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unix socket');
        buildUpstreamUrl(['host' => '/var/run/postgresql', 'port' => '5432', 'database' => 'db']);
    }

    public function testUrlKeyExtractsAllFields(): void
    {
        $url = buildUpstreamUrl([
            'url' => 'postgresql://myuser:mypass@remote.db.com:5433/proddb',
            'host' => 'localhost',
            'port' => '5432',
            'database' => 'wrong',
            'username' => 'wrong',
            'password' => 'wrong',
        ]);
        $this->assertSame('postgresql://myuser:mypass@remote.db.com:5433/proddb', $url);
    }

    public function testUrlKeyOverridesIndividualKeys(): void
    {
        $url = buildUpstreamUrl([
            'url' => 'postgresql://urluser:urlpass@urlhost:6543/urldb',
            'host' => 'individual-host',
            'port' => '1111',
            'database' => 'individual-db',
            'username' => 'individual-user',
            'password' => 'individual-pass',
        ]);
        $this->assertSame('postgresql://urluser:urlpass@urlhost:6543/urldb', $url);
    }

    public function testUrlKeyWithoutPortUsesDefault(): void
    {
        $url = buildUpstreamUrl([
            'url' => 'postgresql://u:p@host/db',
        ]);
        $this->assertSame('postgresql://u:p@host:5432/db', $url);
    }

    public function testUrlKeyWithoutUserInfo(): void
    {
        $url = buildUpstreamUrl([
            'url' => 'postgresql://host:5555/db',
        ]);
        $this->assertSame('postgresql://host:5555/db', $url);
    }

    public function testNullUrlKeyIgnored(): void
    {
        $url = buildUpstreamUrl([
            'url' => null,
            'host' => 'h',
            'port' => '5432',
            'database' => 'db',
            'username' => 'u',
            'password' => 'p',
        ]);
        $this->assertSame('postgresql://u:p@h:5432/db', $url);
    }

    public function testEmptyUrlKeyIgnored(): void
    {
        $url = buildUpstreamUrl([
            'url' => '',
            'host' => 'h',
            'port' => '5432',
            'database' => 'db',
        ]);
        $this->assertSame('postgresql://h:5432/db', $url);
    }

    public function testUrlKeyWithEncodedCredentials(): void
    {
        $url = buildUpstreamUrl([
            'url' => 'postgresql://u%40ser:p%40ss@host:5432/db',
        ]);
        // parseUrlIntoConfig decodes, then buildUpstreamUrl re-encodes
        $this->assertSame('postgresql://u%40ser:p%40ss@host:5432/db', $url);
    }
}
