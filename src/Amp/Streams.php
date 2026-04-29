<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Streams namespace API — `$gl->streams-><verb>(...)` for the async surface.
 *
 * Wraps the wire-level stream methods in a sub-API instance held on the
 * parent GoldLapel\Amp\GoldLapel client. Mirrors goldlapel-python's
 * `goldlapel.streams.StreamsAPI` and the sync `GoldLapel\Streams`.
 */
final class Streams
{
    public function __construct(private readonly GoldLapel $gl) {}

    /**
     * Fetch (and cache) canonical stream DDL + query patterns from the proxy.
     * Cache lives on the parent Amp GoldLapel instance.
     */
    private function patterns(string $stream): array
    {
        \GoldLapel\Utils::validateIdentifier($stream);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'stream',
            $stream,
            $this->gl->getDashboardPort(),
            $token,
        );
    }

    public function add(string $stream, array $payload, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($stream);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::streamAdd($c, $stream, $payload, $patterns));
    }

    public function createGroup(string $stream, string $group, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($stream);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::streamCreateGroup($c, $stream, $group, $patterns));
    }

    public function read(
        string $stream,
        string $group,
        string $consumer,
        int $count = 1,
        ?PostgresExecutor $conn = null,
    ): Future {
        $patterns = $this->patterns($stream);
        $c = $this->gl->resolveConnPublic($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'streams->read requires a PostgresConnection (opens its own transaction).'
            );
        }
        return async(fn() => Utils::streamRead($c, $stream, $group, $consumer, $count, $patterns));
    }

    public function ack(string $stream, string $group, int $messageId, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($stream);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::streamAck($c, $stream, $group, $messageId, $patterns));
    }

    public function claim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
        ?PostgresExecutor $conn = null,
    ): Future {
        $patterns = $this->patterns($stream);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::streamClaim($c, $stream, $group, $consumer, $minIdleMs, $patterns));
    }
}
