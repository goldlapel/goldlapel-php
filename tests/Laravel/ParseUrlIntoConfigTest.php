<?php

namespace GoldLapel\Laravel\Tests;

use PHPUnit\Framework\TestCase as PureTestCase;

use function GoldLapel\Laravel\parseUrlIntoConfig;

class ParseUrlIntoConfigTest extends PureTestCase
{
    public function testParsesFullUrl(): void
    {
        $config = parseUrlIntoConfig([
            'url' => 'postgresql://admin:secret@db.example.com:5433/mydb',
        ]);
        $this->assertSame('db.example.com', $config['host']);
        $this->assertSame('5433', $config['port']);
        $this->assertSame('admin', $config['username']);
        $this->assertSame('secret', $config['password']);
        $this->assertSame('mydb', $config['database']);
    }

    public function testReturnsConfigUnchangedWhenNoUrl(): void
    {
        $original = ['host' => 'h', 'port' => '5432'];
        $config = parseUrlIntoConfig($original);
        $this->assertSame($original, $config);
    }

    public function testReturnsConfigUnchangedWhenUrlNull(): void
    {
        $original = ['url' => null, 'host' => 'h'];
        $config = parseUrlIntoConfig($original);
        $this->assertSame($original, $config);
    }

    public function testReturnsConfigUnchangedWhenUrlEmpty(): void
    {
        $original = ['url' => '', 'host' => 'h'];
        $config = parseUrlIntoConfig($original);
        $this->assertSame($original, $config);
    }

    public function testDecodesPercentEncodedCredentials(): void
    {
        $config = parseUrlIntoConfig([
            'url' => 'postgresql://u%40ser:p%40ss@host:5432/db',
        ]);
        $this->assertSame('u@ser', $config['username']);
        $this->assertSame('p@ss', $config['password']);
    }
}
