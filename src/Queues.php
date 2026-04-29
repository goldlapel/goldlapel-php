<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Queue namespace API — `$gl->queues-><verb>(...)`.
 *
 * Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
 * with visibility-timeout — NOT the legacy fire-and-forget shape. The
 * breaking change:
 *
 *   Before:  $payload = $gl->dequeue('jobs');     // delete-on-fetch, may lose work
 *   After :  $msg = $gl->queues->claim('jobs');   // lease the row
 *            // ['id' => N, 'payload' => mixed] or null
 *            // ... handle the work ...
 *            $gl->queues->ack('jobs', $msg['id']); // commit; missing ack → redelivery
 *
 * `claim` returns `['id' => int, 'payload' => mixed]` or `null`. The caller
 * MUST `ack($id)` to commit, or `abandon($id)` to release the lease
 * immediately. A consumer that crashes leaves the lease standing; the
 * message becomes ready again after `$visibilityTimeoutMs` and is
 * redelivered to the next claim.
 *
 * No `dequeue` shim — the master plan explicitly forbids it; claim+ack
 * is the explicit-by-design surface.
 *
 * Mirrors goldlapel-python's `goldlapel.queues.QueuesAPI`.
 */
final class Queues
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        Utils::validateIdentifier($name);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'queue',
            $name,
            $this->gl->getDashboardPort(),
            $token,
        );
    }

    public function create(string $name): void
    {
        $this->patterns($name);
    }

    public function enqueue(string $name, mixed $payload, ?\PDO $conn = null): ?int
    {
        $patterns = $this->patterns($name);
        return Utils::queueEnqueue(
            $this->gl->resolveConnPublic($conn), $name, $payload, $patterns,
        );
    }

    /**
     * Claim the next ready message; returns
     * `['id' => int, 'payload' => mixed]` or `null` when the queue is empty.
     */
    public function claim(string $name, int $visibilityTimeoutMs = 30000, ?\PDO $conn = null): ?array
    {
        $patterns = $this->patterns($name);
        return Utils::queueClaim(
            $this->gl->resolveConnPublic($conn), $name, $visibilityTimeoutMs, $patterns,
        );
    }

    public function ack(string $name, int $messageId, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::queueAck(
            $this->gl->resolveConnPublic($conn), $name, $messageId, $patterns,
        );
    }

    /**
     * Release a claimed message back to ready immediately so the message is
     * redelivered on the next claim, without waiting for the visibility
     * timeout. Equivalent to a queue NACK.
     */
    public function abandon(string $name, int $messageId, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::queueAbandon(
            $this->gl->resolveConnPublic($conn), $name, $messageId, $patterns,
        );
    }

    /** Push the visibility deadline forward by `$additionalMs` milliseconds. */
    public function extend(string $name, int $messageId, int $additionalMs, ?\PDO $conn = null): mixed
    {
        $patterns = $this->patterns($name);
        return Utils::queueExtend(
            $this->gl->resolveConnPublic($conn), $name, $messageId, $additionalMs, $patterns,
        );
    }

    /** Look at the next-ready message without claiming. */
    public function peek(string $name, ?\PDO $conn = null): ?array
    {
        $patterns = $this->patterns($name);
        return Utils::queuePeek(
            $this->gl->resolveConnPublic($conn), $name, $patterns,
        );
    }

    public function countReady(string $name, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::queueCountReady(
            $this->gl->resolveConnPublic($conn), $name, $patterns,
        );
    }

    public function countClaimed(string $name, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::queueCountClaimed(
            $this->gl->resolveConnPublic($conn), $name, $patterns,
        );
    }
}
