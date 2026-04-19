<?php

namespace GoldLapel\Amp;

use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use GoldLapel\NativeCache;

/**
 * Async L1-cached wrapper around a PostgresExecutor (connection or
 * transaction).
 *
 * Mirrors the sync CachedPDO, with two important differences:
 *
 * 1. The cache layer is purely synchronous — NativeCache is an in-process
 *    hash-table with no I/O. It's safe to read/write from any fiber.
 * 2. Invalidation signals arrive on a Unix/TCP socket the Rust proxy
 *    broadcasts on. Rather than poll in a dedicated fiber, we drain
 *    pending signals opportunistically on every query (same model as
 *    sync CachedPDO) — this keeps dirty data from being served on reads
 *    while staying lock-free.
 *
 * Only the high-traffic read path (SELECT without TX) hits the cache.
 * Writes invalidate the affected tables; DDL invalidates all.
 *
 * This wrapper implements PostgresExecutor so it can be passed anywhere
 * a real executor is accepted — including GoldLapel\Amp\Utils methods
 * and into using() scopes.
 */
class CachedConnection implements PostgresExecutor
{
    use \Amp\ForbidCloning;
    use \Amp\ForbidSerialization;

    private bool $inTransaction = false;

    public function __construct(
        private PostgresExecutor $real,
        private NativeCache $cache,
    ) {
    }

    public function getWrappedExecutor(): PostgresExecutor
    {
        return $this->real;
    }

    public function unwrap(): PostgresExecutor
    {
        return $this->real;
    }

    public function getCache(): NativeCache
    {
        return $this->cache;
    }

    /**
     * Handle cache semantics for a SQL statement:
     *   - detect TX boundaries (bypass cache inside a tx)
     *   - detect writes (invalidate)
     *   - drain pending invalidation signals
     *   - cache SELECTs when unparameterized; return cached PostgresResult
     *     via CachedResult if hit
     */
    private function handle(string $sql, ?array $params, \Closure $miss): PostgresResult
    {
        $this->cache->pollSignals();

        if (NativeCache::isTxStart($sql)) {
            $this->inTransaction = true;
            return $miss();
        }
        if (NativeCache::isTxEnd($sql)) {
            $this->inTransaction = false;
            return $miss();
        }

        $writeTable = NativeCache::detectWrite($sql);
        if ($writeTable !== null) {
            if ($writeTable === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                $this->cache->invalidateTable($writeTable);
            }
            return $miss();
        }

        if ($this->inTransaction) {
            return $miss();
        }

        $entry = $this->cache->get($sql, $params);
        if ($entry !== null) {
            return new CachedResult($entry['rows']);
        }

        $result = $miss();
        // Buffer the rows so we can both cache and return them — amphp
        // Results are iterable-once, so we drain into an array.
        $rows = [];
        foreach ($result as $row) {
            $rows[] = $row;
        }
        $columns = !empty($rows) ? array_keys($rows[0]) : [];
        $this->cache->put($sql, $params, $rows, $columns);
        return new CachedResult($rows);
    }

    // ---- PostgresExecutor / SqlExecutor interface ----

    public function query(string $sql): PostgresResult
    {
        return $this->handle($sql, null, fn() => $this->real->query($sql));
    }

    public function execute(string $sql, array $params = []): PostgresResult
    {
        return $this->handle($sql, $params, fn() => $this->real->execute($sql, $params));
    }

    public function prepare(string $sql): \Amp\Postgres\PostgresStatement
    {
        // Prepared statements bypass the L1 read cache: they're usually
        // parameterized reads we'd cache per-param anyway. Keep simple —
        // just forward. If demand surfaces, wrap PostgresStatement too.
        return $this->real->prepare($sql);
    }

    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        return $this->real->notify($channel, $payload);
    }

    public function quoteLiteral(string $data): string
    {
        return $this->real->quoteLiteral($data);
    }

    public function quoteIdentifier(string $name): string
    {
        return $this->real->quoteIdentifier($name);
    }

    public function escapeByteA(string $data): string
    {
        return $this->real->escapeByteA($data);
    }

    public function isClosed(): bool
    {
        return $this->real->isClosed();
    }

    public function close(): void
    {
        $this->real->close();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->real->onClose($onClose);
    }

    public function getLastUsedAt(): int
    {
        return $this->real->getLastUsedAt();
    }
}
