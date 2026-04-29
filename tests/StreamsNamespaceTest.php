<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Streams;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Streams — the nested `$gl->streams` namespace
 * introduced alongside Phase 4 of schema-to-core. Streams DDL ownership
 * shipped earlier (Phase 1+2); the namespace nesting is the new piece.
 *
 * Mirrors goldlapel-python/tests/test_streams.py.
 */
#[AllowMockObjectsWithoutExpectations]
class StreamsNamespaceTest extends TestCase
{
    private function makeGl(): GoldLapel
    {
        $gl = new GoldLapel('postgresql://user:pass@host:5432/db');
        $pdo = $this->createMock(\PDO::class);
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        $ref->setValue($gl, $pdo);
        return $gl;
    }

    private function seedCache(GoldLapel $gl, string $name, array $patterns): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'ddlCache');
        $ref->setAccessible(true);
        $cache = $ref->getValue($gl);
        $cache["stream:{$name}"] = $patterns;
        $ref->setValue($gl, $cache);
    }

    // ---- Namespace shape ----

    public function testStreamsIsAStreamsInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Streams::class, $gl->streams);
    }

    public function testStreamsIsReadonlyProperty(): void
    {
        $gl = $this->makeGl();
        $this->expectException(\Error::class);
        // @phpstan-ignore-next-line — intentional violation
        $gl->streams = new Streams($gl);
    }

    public function testNoLegacyFlatStreamMethodsOnGl(): void
    {
        // Hard cut — the flat stream* methods are gone.
        $legacy = ['streamAdd', 'streamCreateGroup', 'streamRead', 'streamAck', 'streamClaim'];
        foreach ($legacy as $name) {
            $this->assertFalse(
                method_exists(GoldLapel::class, $name),
                "Legacy flat method GoldLapel::{$name}() should have been removed; "
                . "use \$gl->streams-><verb>() instead."
            );
        }
    }

    // ---- Verb dispatch ----

    public function testAddDispatchesWithPatterns(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'events', [
            'tables' => ['main' => '_goldlapel.stream_events'],
            'query_patterns' => [
                'insert' => 'INSERT INTO _goldlapel.stream_events (payload) VALUES ($1) RETURNING id, created_at',
            ],
        ]);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(7);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                // The proxy emits $1; the wrapper rewrites to ?::jsonb on the
                // INSERT VALUES clause for PDO compatibility.
                $this->assertStringContainsString('INSERT INTO _goldlapel.stream_events', $sql);
                $this->assertStringContainsString('?::jsonb', $sql);
                return true;
            }))
            ->willReturn($stmt);

        $id = $gl->streams->add('events', ['type' => 'click']);
        $this->assertSame(7, $id);
    }

    public function testCreateGroupPassesGroup(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'events', [
            'tables' => ['main' => '_goldlapel.stream_events'],
            'query_patterns' => [
                'create_group' => 'INSERT INTO _goldlapel.stream_events_groups (name, last_id) VALUES ($1, 0) ON CONFLICT DO NOTHING',
            ],
        ]);

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->willReturn($stmt);

        $stmt->expects($this->once())->method('execute')->with(['workers']);

        $gl->streams->createGroup('events', 'workers');
    }

    private function getInternalPdo(GoldLapel $gl): \PDO
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }
}
