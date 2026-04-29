<?php
declare(strict_types=1);

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\Counters;
use GoldLapel\Amp\Documents;
use GoldLapel\Amp\Geos;
use GoldLapel\Amp\GoldLapel;
use GoldLapel\Amp\Hashes;
use GoldLapel\Amp\Queues;
use GoldLapel\Amp\Streams;
use GoldLapel\Amp\Zsets;
use PHPUnit\Framework\TestCase;

/**
 * Sanity checks for the schema-to-core nesting on the async surface.
 * Verifies that the sub-API namespace properties exist, are readonly, and
 * that the legacy flat methods are gone. End-to-end behavior is covered by
 * the integration test suite (which requires a running proxy + Postgres).
 */
class NamespaceShapeTest extends TestCase
{
    public function testDocumentsIsADocumentsInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Documents::class, $gl->documents);
    }

    public function testStreamsIsAStreamsInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Streams::class, $gl->streams);
    }

    public function testCountersIsACountersInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Counters::class, $gl->counters);
    }

    public function testZsetsIsAZsetsInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Zsets::class, $gl->zsets);
    }

    public function testHashesIsAHashesInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Hashes::class, $gl->hashes);
    }

    public function testQueuesIsAQueuesInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Queues::class, $gl->queues);
    }

    public function testGeosIsAGeosInstance(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->assertInstanceOf(Geos::class, $gl->geos);
    }

    public function testDocumentsIsReadonly(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->documents = new Documents($gl);
    }

    public function testCountersIsReadonly(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->counters = new Counters($gl);
    }

    public function testNoLegacyFlatDocMethods(): void
    {
        $legacy = [
            'docInsert', 'docInsertMany', 'docFind', 'docFindOne', 'docFindCursor',
            'docUpdate', 'docUpdateOne', 'docDelete', 'docDeleteOne',
            'docFindOneAndUpdate', 'docFindOneAndDelete', 'docDistinct',
            'docCount', 'docCreateIndex', 'docAggregate', 'docWatch', 'docUnwatch',
            'docCreateTtlIndex', 'docRemoveTtlIndex', 'docCreateCollection',
            'docCreateCapped', 'docRemoveCap',
        ];
        foreach ($legacy as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Legacy flat method GoldLapel\\Amp\\GoldLapel::{$name}() should have been "
                . "removed; use \$gl->documents-><verb>() instead."
            );
        }
    }

    public function testNoLegacyFlatStreamMethods(): void
    {
        $legacy = ['streamAdd', 'streamCreateGroup', 'streamRead', 'streamAck', 'streamClaim'];
        foreach ($legacy as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Legacy flat method GoldLapel\\Amp\\GoldLapel::{$name}() should have been "
                . "removed; use \$gl->streams-><verb>() instead."
            );
        }
    }

    public function testNoLegacyFlatCounterMethods(): void
    {
        foreach (['incr', 'getCounter'] as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Phase 5 removed flat {$name}() — use \$gl->counters-><verb>()."
            );
        }
    }

    public function testNoLegacyFlatZsetMethods(): void
    {
        foreach (['zadd', 'zincrby', 'zrange', 'zrank', 'zscore', 'zrem'] as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Phase 5 removed flat {$name}() — use \$gl->zsets-><verb>()."
            );
        }
    }

    public function testNoLegacyFlatHashMethods(): void
    {
        foreach (['hset', 'hget', 'hgetall', 'hdel'] as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Phase 5 removed flat {$name}() — use \$gl->hashes-><verb>()."
            );
        }
    }

    public function testNoLegacyFlatQueueMethods(): void
    {
        foreach (['enqueue', 'dequeue'] as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Phase 5 removed flat {$name}() — use \$gl->queues-><verb>(); "
                . "no `dequeue` shim, claim+ack is explicit by design."
            );
        }
    }

    public function testNoLegacyFlatGeoMethods(): void
    {
        foreach (['geoadd', 'georadius', 'geodist'] as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Phase 5 removed flat {$name}() — use \$gl->geos-><verb>()."
            );
        }
    }
}
