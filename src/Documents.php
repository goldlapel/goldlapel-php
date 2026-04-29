<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * Documents namespace API — `$gl->documents-><verb>(...)`.
 *
 * Wraps the doc-store methods in a sub-API instance held on the parent
 * GoldLapel client. The instance shares all state (license, dashboard token,
 * PDO, DDL pattern cache) by reference back to the parent — no duplication.
 *
 * The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call here:
 *
 *   1. Calls `/api/ddl/doc_store/create` (idempotent) to materialize the
 *      canonical `_goldlapel.doc_<name>` table and pull its query patterns.
 *   2. Caches `(tables, query_patterns)` on the parent GoldLapel instance for
 *      the session's lifetime (one HTTP round-trip per (family, name) per
 *      session).
 *   3. Hands the patterns off to the existing `GoldLapel\Utils::doc*` static
 *      helpers so they execute against the canonical table name instead of
 *      CREATE-ing their own.
 *
 * Sub-API class shape mirrors `GoldLapel\Streams` — this is the canonical
 * pattern for the wrapper rollout. Other namespaces (cache, search, queues,
 * counters, hashes, zsets, geo, auth, ...) stay flat for now; they migrate
 * to nested form one-at-a-time as their own schema-to-core phase fires.
 *
 * Mirrors goldlapel-python's `goldlapel.documents.DocumentsAPI`.
 */
final class Documents
{
    /**
     * Hold a back-reference to the parent client. Never copy lifecycle state
     * (token, port, PDO) onto this instance — always read through the parent
     * so a config change on the parent (e.g. proxy restart with a new
     * dashboard token) is reflected immediately on the next call.
     */
    public function __construct(private readonly GoldLapel $gl) {}

    /**
     * Fetch (and cache) canonical doc-store DDL + query patterns from the
     * proxy. Cache lives on the parent GoldLapel instance.
     *
     * `$unlogged` is a creation-time option; passed only on the first call
     * for a given (family, name) since proxy `CREATE TABLE IF NOT EXISTS`
     * makes subsequent calls no-op DDL-wise. If a caller flips `$unlogged`
     * across calls in the same session, the table's storage type is
     * whatever it was on first create — wrappers don't migrate it.
     */
    private function patterns(string $collection, bool $unlogged = false): array
    {
        Utils::validateIdentifier($collection);
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
     * Eagerly materialize the doc-store table. Other methods will also
     * materialize on first use, so calling this is optional — provided for
     * callers that want explicit setup at startup time.
     */
    public function createCollection(string $collection, bool $unlogged = false): void
    {
        $this->patterns($collection, $unlogged);
        // Nothing else to do: the proxy already executed the DDL on its mgmt
        // connection.
    }

    // -- CRUD ----------------------------------------------------------------

    public function insert(string $collection, array $document, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($collection);
        return Utils::docInsert(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $document,
            $patterns,
        );
    }

    public function insertMany(string $collection, array $documents, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($collection);
        return Utils::docInsertMany(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $documents,
            $patterns,
        );
    }

    public function find(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        ?\PDO $conn = null,
    ): array {
        $patterns = $this->patterns($collection);
        return Utils::docFind(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $sort,
            $limit,
            $skip,
            $patterns,
        );
    }

    public function findCursor(
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        int $batchSize = 100,
        ?\PDO $conn = null,
    ): \Generator {
        $patterns = $this->patterns($collection);
        return Utils::docFindCursor(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $sort,
            $limit,
            $skip,
            $batchSize,
            $patterns,
        );
    }

    public function findOne(string $collection, ?array $filter = null, ?\PDO $conn = null): ?array
    {
        $patterns = $this->patterns($collection);
        return Utils::docFindOne(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $patterns,
        );
    }

    public function update(string $collection, array $filter, array $update, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($collection);
        return Utils::docUpdate(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $update,
            $patterns,
        );
    }

    public function updateOne(string $collection, array $filter, array $update, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($collection);
        return Utils::docUpdateOne(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $update,
            $patterns,
        );
    }

    public function delete(string $collection, array $filter, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($collection);
        return Utils::docDelete(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $patterns,
        );
    }

    public function deleteOne(string $collection, array $filter, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($collection);
        return Utils::docDeleteOne(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $patterns,
        );
    }

    public function findOneAndUpdate(
        string $collection,
        array $filter,
        array $update,
        ?\PDO $conn = null,
    ): ?array {
        $patterns = $this->patterns($collection);
        return Utils::docFindOneAndUpdate(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $update,
            $patterns,
        );
    }

    public function findOneAndDelete(string $collection, array $filter, ?\PDO $conn = null): ?array
    {
        $patterns = $this->patterns($collection);
        return Utils::docFindOneAndDelete(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $patterns,
        );
    }

    public function distinct(
        string $collection,
        string $field,
        ?array $filter = null,
        ?\PDO $conn = null,
    ): array {
        $patterns = $this->patterns($collection);
        return Utils::docDistinct(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $field,
            $filter,
            $patterns,
        );
    }

    public function count(string $collection, ?array $filter = null, ?\PDO $conn = null): int
    {
        $patterns = $this->patterns($collection);
        return Utils::docCount(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $filter,
            $patterns,
        );
    }

    public function createIndex(string $collection, ?array $keys = null, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($collection);
        Utils::docCreateIndex(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $keys,
            $patterns,
        );
    }

    /**
     * Run a Mongo-style aggregation pipeline.
     *
     * `$lookup.from` references are resolved to their canonical proxy tables
     * (`_goldlapel.doc_<name>`) — each unique `from` collection triggers an
     * idempotent describe/create against the proxy and is cached for the
     * session.
     */
    public function aggregate(string $collection, array $pipeline, ?\PDO $conn = null): array
    {
        $patterns = $this->patterns($collection);
        // Walk the pipeline once to find every $lookup.from collection, fetch
        // patterns for each (cached after first call), and pass the resolved
        // map down to docAggregate.
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
        return Utils::docAggregate(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $pipeline,
            $patterns,
            $lookupTables,
        );
    }

    // -- Watch / TTL / capped ------------------------------------------------

    public function watch(string $collection, ?callable $callback = null, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($collection);
        Utils::docWatch(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $callback,
            $patterns,
        );
    }

    public function unwatch(string $collection, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($collection);
        Utils::docUnwatch(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $patterns,
        );
    }

    public function createTtlIndex(
        string $collection,
        int $expireAfterSeconds,
        string $field = 'created_at',
        ?\PDO $conn = null,
    ): void {
        $patterns = $this->patterns($collection);
        Utils::docCreateTtlIndex(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $expireAfterSeconds,
            $field,
            $patterns,
        );
    }

    public function removeTtlIndex(string $collection, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($collection);
        Utils::docRemoveTtlIndex(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $patterns,
        );
    }

    public function createCapped(string $collection, int $maxDocuments, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($collection);
        Utils::docCreateCapped(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $maxDocuments,
            $patterns,
        );
    }

    public function removeCap(string $collection, ?\PDO $conn = null): void
    {
        $patterns = $this->patterns($collection);
        Utils::docRemoveCap(
            $this->gl->resolveConnPublic($conn),
            $collection,
            $patterns,
        );
    }
}
