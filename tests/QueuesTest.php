<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use GoldLapel\GoldLapel;
use GoldLapel\Queues;
use GoldLapel\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for GoldLapel\Queues — the nested `$gl->queues` namespace.
 *
 * Phase 5 introduced at-least-once delivery with visibility-timeout. The
 * breaking change is `dequeue` (delete-on-fetch) → `claim` (lease + ack).
 * These tests verify:
 *
 *   - `enqueue` returns the assigned id from the proxy's RETURNING clause.
 *   - `claim` returns `['id' => int, 'payload' => mixed]` or `null`.
 *   - `ack` is a separate call, NOT bundled into claim.
 *   - `abandon` / `nack` releases the claim immediately.
 *   - `extend` pushes the visibility deadline.
 *   - `gl->dequeue` is gone; `gl->queues->dequeue` is also gone (no shim).
 *
 * Mirrors goldlapel-python/tests/test_queues.py.
 */
#[AllowMockObjectsWithoutExpectations]
class QueuesTest extends TestCase
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

    private function fakePatterns(): array
    {
        $main = '_goldlapel.queue_jobs';
        return [
            'tables' => ['main' => $main],
            'query_patterns' => [
                'enqueue' => "INSERT INTO {$main} (payload) VALUES (\$1::jsonb) RETURNING id, created_at",
                'claim' => "WITH next_msg AS ( SELECT id FROM {$main} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1 ) UPDATE {$main} SET status = 'claimed', visible_at = NOW() + INTERVAL '1 millisecond' * \$1 FROM next_msg WHERE {$main}.id = next_msg.id RETURNING {$main}.id, {$main}.payload, {$main}.visible_at, {$main}.created_at",
                'ack' => "DELETE FROM {$main} WHERE id = \$1",
                'extend' => "WITH target AS (SELECT \$1::bigint AS id, \$2::bigint AS additional_ms) UPDATE {$main} m SET visible_at = m.visible_at + INTERVAL '1 millisecond' * target.additional_ms FROM target WHERE m.id = target.id AND m.status = 'claimed' RETURNING m.visible_at",
                'nack' => "UPDATE {$main} SET status = 'ready', visible_at = NOW() WHERE id = \$1 AND status = 'claimed' RETURNING id",
                'peek' => "SELECT id, payload, visible_at, status, created_at FROM {$main} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id LIMIT 1",
                'count_ready' => "SELECT COUNT(*) FROM {$main} WHERE status = 'ready' AND visible_at <= NOW()",
                'count_claimed' => "SELECT COUNT(*) FROM {$main} WHERE status = 'claimed'",
            ],
        ];
    }

    private function seedCache(GoldLapel $gl, string $family, string $name, array $patterns): void
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'ddlCache');
        $ref->setAccessible(true);
        $cache = $ref->getValue($gl);
        $cache["{$family}:{$name}"] = $patterns;
        $ref->setValue($gl, $cache);
    }

    private function getInternalPdo(GoldLapel $gl): \PDO
    {
        $ref = new \ReflectionProperty(GoldLapel::class, 'pdo');
        $ref->setAccessible(true);
        return $ref->getValue($gl);
    }

    // ---- Namespace shape ----

    public function testQueuesIsAQueuesInstance(): void
    {
        $gl = $this->makeGl();
        $this->assertInstanceOf(Queues::class, $gl->queues);
    }

    public function testNoLegacyFlatQueueMethods(): void
    {
        $gl = $this->makeGl();
        foreach (['enqueue', 'dequeue'] as $name) {
            $this->assertFalse(
                method_exists($gl, $name),
                "Phase 5 removed flat {$name} — use \$gl->queues-><verb>()."
            );
        }
    }

    public function testNoDequeueAliasOnQueuesNamespace(): void
    {
        $gl = $this->makeGl();
        // The dispatcher considered a `dequeue` compat shim that combined
        // claim+ack. Master plan rejected that — claim+ack is explicit by
        // design.
        $this->assertFalse(
            method_exists($gl->queues, 'dequeue'),
            'Phase 5 forbids a dequeue alias — claim+ack is explicit by design.'
        );
    }

    // ---- Verb dispatch ----

    public function testEnqueueReturnsIdFromProxy(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([99, '2026-04-30T00:00']);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['{"x":1}']);

        $this->assertSame(99, $gl->queues->enqueue('jobs', ['x' => 1]));
    }

    public function testClaimReturnsAssocOrNull(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([7, '{"x":1}', '2026-04-30T00:00', '2026-04-30T00:00']);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(
            ['id' => 7, 'payload' => ['x' => 1]],
            $gl->queues->claim('jobs'),
        );
    }

    public function testClaimReturnsNullWhenEmpty(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertNull($gl->queues->claim('jobs'));
    }

    public function testClaimAndAckAreDistinctCalls(): void
    {
        // Phase-5 contract: the claim SQL must NOT contain DELETE.
        $sql = $this->fakePatterns()['query_patterns']['claim'];
        $this->assertStringNotContainsStringIgnoringCase('DELETE', $sql);
    }

    public function testAckReturnsTrueWhenDeleted(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertTrue($gl->queues->ack('jobs', 42));
    }

    public function testAbandonUsesNackPattern(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([42]);

        $pdo = $this->getInternalPdo($gl);
        $pdo->expects($this->once())
            ->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("status = 'ready'", $sql);
                return true;
            }))
            ->willReturn($stmt);

        $this->assertTrue($gl->queues->abandon('jobs', 42));
    }

    public function testExtendBindsIdAndAdditionalMs(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(['2026-05-01T00:00']);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);
        // Proxy contract: $1=id, $2=additional_ms (source order). After
        // $N → ?, the bindings appear in source order.
        $stmt->expects($this->once())
            ->method('execute')
            ->with([42, 5000]);

        $this->assertSame('2026-05-01T00:00', $gl->queues->extend('jobs', 42, 5000));
    }

    public function testPeekReturnsAssoc(): void
    {
        $gl = $this->makeGl();
        $this->seedCache($gl, 'queue', 'jobs', $this->fakePatterns());

        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn([42, '{"work":"foo"}', 'vat', 'ready', 'cat']);

        $pdo = $this->getInternalPdo($gl);
        $pdo->method('prepare')->willReturn($stmt);

        $this->assertSame(
            [
                'id' => 42,
                'payload' => ['work' => 'foo'],
                'visible_at' => 'vat',
                'status' => 'ready',
                'created_at' => 'cat',
            ],
            $gl->queues->peek('jobs'),
        );
    }
}
