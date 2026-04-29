<?php
declare(strict_types=1);

namespace GoldLapel\Amp;

use Amp\Future;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use GoldLapel\Ddl;

use function Amp\async;

/**
 * Documents namespace API — `$gl->documents-><verb>(...)` for the async surface.
 *
 * Wraps the doc-store methods in a sub-API instance held on the parent
 * GoldLapel\Amp\GoldLapel client. Each verb fetches canonical DDL + query
 * patterns from the proxy on first use of a collection (cached for the
 * session) and dispatches to the static GoldLapel\Amp\Utils helpers.
 *
 * Mirrors goldlapel-python's `goldlapel.documents.DocumentsAPI` and the
 * sync `GoldLapel\Documents`.
 */
final class Documents
{
    public function __construct(private readonly GoldLapel $gl) {}

    /**
     * Fetch (and cache) canonical doc-store DDL + query patterns from the
     * proxy. Cache lives on the parent Amp GoldLapel instance.
     */
    private function patterns(string $collection, bool $unlogged = false): array
    {
        \GoldLapel\Utils::validateIdentifier($collection);
        $token = $this->gl->dashboardToken() ?? Ddl::tokenFromEnvOrFile();
        $cache = &$this->gl->ddlCache();
        $options = $unlogged ? ['unlogged' => true] : null;
        return Ddl::fetchPatterns(
            $cache,
            'doc_store',
            $collection,
            $this->gl->getDashboardPort(),
            $token,
            $options,
        );
    }

    // -- Collection lifecycle ------------------------------------------------

    /**
     * Eagerly materialize the doc-store table. Returns a Future<void> for
     * surface-symmetry with the other verbs even though the materialization
     * happened synchronously inside the dashboard call.
     */
    public function createCollection(string $collection, bool $unlogged = false): Future
    {
        return async(function () use ($collection, $unlogged): void {
            $this->patterns($collection, $unlogged);
        });
    }

    // -- CRUD ----------------------------------------------------------------

    public function insert(string $collection, array $document, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docInsert($c, $collection, $document, $patterns));
    }

    public function insertMany(string $collection, array $documents, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docInsertMany($c, $collection, $documents, $patterns));
    }

    public function find(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        ?PostgresExecutor $conn = null,
    ): Future {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docFind($c, $collection, $filter, $sort, $limit, $skip, $patterns));
    }

    /**
     * Returns an Amp\Pipeline\ConcurrentIterator<array> — iterate with foreach
     * inside a fiber. Returns the iterator synchronously (no Future wrap)
     * because the iterator itself is the awaitable.
     */
    public function findCursor(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        int $batchSize = 100,
        ?PostgresExecutor $conn = null,
    ): \Amp\Pipeline\ConcurrentIterator {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'documents->findCursor requires a PostgresConnection (cursor-in-transaction).'
            );
        }
        return Utils::docFindCursor($c, $collection, $filter, $sort, $limit, $skip, $batchSize, $patterns);
    }

    public function findOne(string $collection, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docFindOne($c, $collection, $filter, $patterns));
    }

    public function update(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docUpdate($c, $collection, $filter, $update, $patterns));
    }

    public function updateOne(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docUpdateOne($c, $collection, $filter, $update, $patterns));
    }

    public function delete(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docDelete($c, $collection, $filter, $patterns));
    }

    public function deleteOne(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docDeleteOne($c, $collection, $filter, $patterns));
    }

    public function findOneAndUpdate(string $collection, array $filter, array $update, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docFindOneAndUpdate($c, $collection, $filter, $update, $patterns));
    }

    public function findOneAndDelete(string $collection, array $filter, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docFindOneAndDelete($c, $collection, $filter, $patterns));
    }

    public function distinct(
        string $collection,
        string $field,
        ?array $filter = null,
        ?PostgresExecutor $conn = null,
    ): Future {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docDistinct($c, $collection, $field, $filter, $patterns));
    }

    public function count(string $collection, ?array $filter = null, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docCount($c, $collection, $filter, $patterns));
    }

    public function createIndex(string $collection, ?array $keys = null, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docCreateIndex($c, $collection, $keys, $patterns));
    }

    /**
     * Run a Mongo-style aggregation pipeline.
     *
     * `$lookup.from` references are resolved to their canonical proxy tables
     * (`_goldlapel.doc_<name>`) — each unique `from` collection triggers an
     * idempotent describe/create against the proxy and is cached for the
     * session.
     */
    public function aggregate(string $collection, array $pipeline, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $lookupTables = [];
        foreach ($pipeline as $stage) {
            if (is_array($stage) && isset($stage['$lookup'])) {
                $spec = $stage['$lookup'];
                if (is_array($spec) && isset($spec['from'])) {
                    $fromName = $spec['from'];
                    if (!isset($lookupTables[$fromName])) {
                        $lp = $this->patterns($fromName);
                        $lookupTables[$fromName] = $lp['tables']['main'];
                    }
                }
            }
        }
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docAggregate($c, $collection, $pipeline, $patterns, $lookupTables));
    }

    // -- Watch / TTL / capped ------------------------------------------------

    /**
     * Install change-stream trigger. If $callback is non-null the future
     * blocks until the listener stops; this requires a full PostgresConnection
     * as the executor (LISTEN is not supported on transactions).
     */
    public function watch(string $collection, ?callable $callback = null, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        if (!$c instanceof PostgresConnection) {
            throw new \InvalidArgumentException(
                'documents->watch requires a PostgresConnection (LISTEN is not supported on transactions).'
            );
        }
        return async(fn() => Utils::docWatch($c, $collection, $callback, $patterns));
    }

    public function unwatch(string $collection, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docUnwatch($c, $collection, $patterns));
    }

    public function createTtlIndex(
        string $collection,
        int $expireAfterSeconds,
        string $field = 'created_at',
        ?PostgresExecutor $conn = null,
    ): Future {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docCreateTtlIndex($c, $collection, $expireAfterSeconds, $field, $patterns));
    }

    public function removeTtlIndex(string $collection, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docRemoveTtlIndex($c, $collection, $patterns));
    }

    public function createCapped(string $collection, int $maxDocuments, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docCreateCapped($c, $collection, $maxDocuments, $patterns));
    }

    public function removeCap(string $collection, ?PostgresExecutor $conn = null): Future
    {
        $patterns = $this->patterns($collection);
        $c = $this->gl->resolveConnPublic($conn);
        return async(fn() => Utils::docRemoveCap($c, $collection, $patterns));
    }
}
