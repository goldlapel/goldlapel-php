<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Queue namespace API — `$gl->queues-><verb>(...)` for the async surface.
 *
 * Phase 5 at-least-once with visibility-timeout. Mirrors
 * `goldlapel.queues.QueuesAPI` and the sync `GoldLapel\Queues`.
 *
 * `claim` returns `['id' => int, 'payload' => mixed]` or `null`. The caller
 * MUST `ack($id)` to commit, or `abandon($id)` to release the lease
 * immediately. No `dequeue` shim — claim+ack is explicit by design.
 */
final class Queues
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        \GoldLapel\Utils::validateIdentifier($name);
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

    public function create(string $name): Future
    {
        return async(function () use ($name): void {
            $this->patterns($name);
        });
    }

    public function enqueue(string $name, mixed $payload, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueEnqueue($c, $name, $payload, $patterns));
    }

    public function claim(string $name, int $visibilityTimeoutMs = 30000, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueClaim($c, $name, $visibilityTimeoutMs, $patterns));
    }

    public function ack(string $name, int $messageId, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueAck($c, $name, $messageId, $patterns));
    }

    public function abandon(string $name, int $messageId, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueAbandon($c, $name, $messageId, $patterns));
    }

    public function extend(string $name, int $messageId, int $additionalMs, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueExtend($c, $name, $messageId, $additionalMs, $patterns));
    }

    public function peek(string $name, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queuePeek($c, $name, $patterns));
    }

    public function countReady(string $name, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueCountReady($c, $name, $patterns));
    }

    public function countClaimed(string $name, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($name);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::queueCountClaimed($c, $name, $patterns));
    }
}
