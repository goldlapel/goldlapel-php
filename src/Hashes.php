<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Hash namespace API — `$gl->hashes-><verb>(...)`.
 *
 * Phase 5 of schema-to-core. The proxy's v1 hash schema is row-per-field
 * (`hash_key`, `field`, `value`) — NOT the legacy JSONB-blob-per-key shape.
 * Every method threads `$hashKey` as the first positional arg after the
 * namespace `$name`. `$value` is JSON-encoded so callers can store arbitrary
 * structured payloads.
 *
 * Mirrors goldlapel-python's `goldlapel.hashes.HashesAPI`.
 */
final class Hashes
{
    public function __construct(private readonly GoldLapel $gl) {}

    private function patterns(string $name): array
    {
        Utils::validateIdentifier($name);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        return Ddl::fetchPatterns(
            $cache,
            'hash',
            $name,
            $this->gl->getDashboardPort(),
            $token,
        );
    }

    public function create(string $name): void
    {
        $this->patterns($name);
    }

    public function set(string $name, string $hashKey, string $field, mixed $value, ?\PDO $conn = null): mixed
    {
        $patterns = $this->patterns($name);
        return Utils::hashSet(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $field, $value, $patterns,
        );
    }

    public function get(string $name, string $hashKey, string $field, ?\PDO $conn = null): mixed
    {
        $patterns = $this->patterns($name);
        return Utils::hashGet(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $field, $patterns,
        );
    }

    public function getAll(string $name, string $hashKey, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($name);
        return Utils::hashGetAll(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $patterns,
        );
    }

    public function keys(string $name, string $hashKey, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($name);
        return Utils::hashKeys(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $patterns,
        );
    }

    public function values(string $name, string $hashKey, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($name);
        return Utils::hashValues(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $patterns,
        );
    }

    public function exists(string $name, string $hashKey, string $field, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::hashExists(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $field, $patterns,
        );
    }

    public function delete(string $name, string $hashKey, string $field, ?\PDO $conn = null): bool
    {
        $patterns = $this->patterns($name);
        return Utils::hashDelete(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $field, $patterns,
        );
    }

    public function len(string $name, string $hashKey, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($name);
        return Utils::hashLen(
            $this->gl->resolveConnPublic($conn), $name, $hashKey, $patterns,
        );
    }
}
