<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Streams namespace API — `$gl->streams-><verb>(...)`.
 *
 * Wraps the wire-level stream methods in a sub-API instance held on the parent
 * GoldLapel client. The instance shares all state (license, dashboard token,
 * PDO, DDL pattern cache) by reference back to the parent — no duplication.
 *
 * This is the canonical sub-API shape for the schema-to-core wrapper rollout.
 * Other namespaces (cache, search, queues, counters, hashes, zsets, geo, auth,
 * ...) stay flat for now; they migrate to nested form one-at-a-time as their
 * own schema-to-core phase fires.
 *
 * Mirrors goldlapel-python's `goldlapel.streams.StreamsAPI`.
 */
final class Streams
{
    /**
     * Hold a back-reference to the parent client. Never copy lifecycle state
     * (token, port, PDO) onto this instance — always read through the parent
     * so a config change on the parent (e.g. proxy restart with a new
     * dashboard token) is reflected immediately on the next call.
     */
    public function __construct(private readonly GoldLapel $gl) {}

    /**
     * Fetch (and cache) canonical stream DDL + query patterns from the proxy.
     * Cache lives on the parent GoldLapel instance.
     */
    private function patterns(string $stream): array
    {
        Utils::validateIdentifier($stream);
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

    public function add(string $stream, array $payload, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($stream);
        return Utils::streamAdd($this->gl->resolveConnPublic($conn), $stream, $payload, $patterns);
    }

    public function createGroup(string $stream, string $group, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($stream);
        Utils::streamCreateGroup($this->gl->resolveConnPublic($conn), $stream, $group, $patterns);
    }

    public function read(
        string $stream,
        string $group,
        string $consumer,
        int $count = 1,
        ?\PDO $conn = null,
    ): array {
        $patterns = $this->patterns($stream);
        return Utils::streamRead(
            $this->gl->resolveConnPublic($conn),
            $stream,
            $group,
            $consumer,
            $count,
            $patterns,
        );
    }

    public function ack(string $stream, string $group, int $messageId, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($stream);
        return Utils::streamAck(
            $this->gl->resolveConnPublic($conn),
            $stream,
            $group,
            $messageId,
            $patterns,
        );
    }

    public function claim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
        ?\PDO $conn = null,
    ): array {
        $patterns = $this->patterns($stream);
        return Utils::streamClaim(
            $this->gl->resolveConnPublic($conn),
            $stream,
            $group,
            $consumer,
            $minIdleMs,
            $patterns,
        );
    }
}
