<?php
declare(strict_types=1);

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\Documents;
use GoldLapel\Amp\GoldLapel;
use GoldLapel\Amp\Streams;
use PHPUnit\Framework\TestCase;

/**
 * Sanity checks for the Phase 4 schema-to-core nesting on the async surface.
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

    public function testDocumentsIsReadonly(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->documents = new Documents($gl);
    }

    public function testStreamsIsReadonly(): void
    {
        $gl = new GoldLapel('postgres://user:pass@host:5432/db');
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->streams = new Streams($gl);
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
}
