<?php

namespace GoldLapel\Amp;

use Amp\Pipeline\Queue;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use Amp\Sql\SqlExecutor;
use GoldLapel\Utils as SyncUtils;

/**
 * Native-amphp versions of the utility functions in GoldLapel\Utils.
 *
 * Every sync util in GoldLapel\Utils that the wrapper methods call has an
 * async sibling here. The SQL strings, identifier validation, filter/update
 * builders, and JSON encoding logic are shared with the sync module — we
 * reuse them directly via the @internal helpers exposed on SyncUtils.
 * Only the driver calls (prepare/execute/fetch/transaction) change.
 *
 * amphp/postgres differences handled here:
 *
 *   - Placeholder translation: amphp/postgres uses `$1, $2, ...` not PDO's
 *     `?`. The sync SQL strings are copied verbatim and `translate($sql, $params)`
 *     translates them on the way out. This keeps the two implementations
 *     in sync without maintaining two copies of every SQL string.
 *
 *   - Named params: amphp/postgres `execute()` takes either a positional
 *     list (keys 0, 1, 2...) OR an associative array (keys match `:name`
 *     placeholders). We always translate `?` to `$N`, and pass params as a
 *     positional list-shaped array. Empty params => empty `[]`.
 *
 *   - No explicit commit: bare Connection `execute()` auto-commits each
 *     statement. The sync path calls `$pdo->beginTransaction()` in a few
 *     places (docFindCursor, streamRead) — the async path uses
 *     `$conn->beginTransaction()` which returns a PostgresTransaction
 *     executor that's committed/rolled back explicitly.
 *
 *   - JSONB result shape: amphp/postgres returns `jsonb` columns as strings
 *     (not decoded). Sync wraps the same PDO behavior. All methods that
 *     return rows with a `data` column preserve the sync return shape
 *     (leaving `data` as a JSON string) — callers json_decode if needed.
 *     Exception: doc* methods that decode `payload` in sync (streamRead,
 *     streamClaim) decode here too.
 *
 *   - LISTEN/NOTIFY: uses `PostgresConnection::listen($channel)` which
 *     returns a Traversable<PostgresNotification>. docFindCursor returns
 *     an Amp\Pipeline for streaming results.
 *
 *   - All public methods accept a `PostgresExecutor` (connection or
 *     transaction) and are synchronous from the caller's point of view
 *     inside a fiber — amphp operations suspend the fiber and resume
 *     when data arrives. Callers wrap in Amp\async() if they want a
 *     Future<T>; the GoldLapel\Amp\GoldLapel facade does so automatically.
 */
class Utils
{
    /**
     * Translate SQL for amphp/postgres. Historical name — today the
     * function mostly exists to pre-rewrite the JSONB `data ? ?` operator
     * pattern (emitted by shared SyncUtils::buildFilter for `$exists` and
     * by hdel) into the function form jsonb_exists(data, ?). amphp's own
     * SQL parser is already smart enough to distinguish `?` placeholders
     * from JSONB `?` operators in most cases (see STATEMENT_PARAM_REGEX in
     * vendor/amphp/postgres), but `data ? ?` is an edge case where an
     * operator `?` is immediately followed by a placeholder `?` with
     * nothing between them — the regex can't tell which is which. The
     * pre-rewrite is the only SQL-level divergence from sync.
     *
     * After the rewrite we STILL translate bare `?` → `$N` ourselves so
     * callers downstream don't have to worry about amphp's placeholder
     * counter — identical shape to the sync PDO path.
     *
     * @param list<mixed> $params
     * @return array{0: string, 1: list<mixed>} translated SQL + params
     */
    public static function translate(string $sql, array $params): array
    {
        // Rewrite JSONB `?` operator patterns produced by shared buildFilter
        // and hdel to the function form jsonb_exists(expr, text). Only the
        // exact patterns `data ? ?` (inside an expression) are emitted by
        // the shared builders, so narrow matching is enough.
        $sql = preg_replace('/\bdata\s*\?\s*\?/', 'jsonb_exists(data, ?)', $sql);

        if ($params === []) {
            // No params: nothing to translate. But some sync sites pass no
            // params yet still use `data ? ?` with the `?` JSONB operator.
            // Those sites DO supply params. If params is empty, the SQL
            // should not contain bound `?` slots, so leaving it alone is
            // safe.
            return [$sql, []];
        }

        $out = '';
        $i = 0;
        $len = strlen($sql);
        $idx = 1;
        $paramCount = count($params);
        $inSingle = false;
        $inDouble = false;
        $inLineComment = false;
        $inBlockComment = false;
        $inDollar = false;
        $dollarTag = '';

        while ($i < $len) {
            $ch = $sql[$i];

            // Handle dollar-quoted strings ($tag$...$tag$) — anything inside
            // is literal. Used by pllua function bodies in script().
            if ($inDollar) {
                if (substr($sql, $i, strlen($dollarTag)) === $dollarTag) {
                    $out .= $dollarTag;
                    $i += strlen($dollarTag);
                    $inDollar = false;
                    $dollarTag = '';
                    continue;
                }
                $out .= $ch;
                $i++;
                continue;
            }

            if ($inLineComment) {
                $out .= $ch;
                if ($ch === "\n") {
                    $inLineComment = false;
                }
                $i++;
                continue;
            }

            if ($inBlockComment) {
                $out .= $ch;
                if ($ch === '*' && $i + 1 < $len && $sql[$i + 1] === '/') {
                    $out .= '/';
                    $i += 2;
                    $inBlockComment = false;
                    continue;
                }
                $i++;
                continue;
            }

            if ($inSingle) {
                $out .= $ch;
                // '' is an escaped single quote
                if ($ch === "'" && $i + 1 < $len && $sql[$i + 1] === "'") {
                    $out .= "'";
                    $i += 2;
                    continue;
                }
                if ($ch === "'") {
                    $inSingle = false;
                }
                $i++;
                continue;
            }

            if ($inDouble) {
                $out .= $ch;
                if ($ch === '"') {
                    $inDouble = false;
                }
                $i++;
                continue;
            }

            // Dollar-quote start
            if ($ch === '$') {
                // $tag$ where tag is [A-Za-z_][A-Za-z_0-9]*
                $j = $i + 1;
                while ($j < $len) {
                    $tc = $sql[$j];
                    if (ctype_alnum($tc) || $tc === '_') {
                        $j++;
                        continue;
                    }
                    break;
                }
                if ($j < $len && $sql[$j] === '$') {
                    $tag = substr($sql, $i, $j - $i + 1);
                    $out .= $tag;
                    $i = $j + 1;
                    $inDollar = true;
                    $dollarTag = $tag;
                    continue;
                }
                // Bare $ (shouldn't occur in our SQL), treat as literal
                $out .= $ch;
                $i++;
                continue;
            }

            if ($ch === "'") {
                $inSingle = true;
                $out .= $ch;
                $i++;
                continue;
            }

            if ($ch === '"') {
                $inDouble = true;
                $out .= $ch;
                $i++;
                continue;
            }

            if ($ch === '-' && $i + 1 < $len && $sql[$i + 1] === '-') {
                $inLineComment = true;
                $out .= '--';
                $i += 2;
                continue;
            }

            if ($ch === '/' && $i + 1 < $len && $sql[$i + 1] === '*') {
                $inBlockComment = true;
                $out .= '/*';
                $i += 2;
                continue;
            }

            if ($ch === '?') {
                $out .= '$' . $idx;
                $idx++;
                $i++;
                continue;
            }

            $out .= $ch;
            $i++;
        }

        if ($idx - 1 !== $paramCount) {
            throw new \InvalidArgumentException(
                "amphp placeholder translation: SQL has " . ($idx - 1)
                . " ? placeholders but got {$paramCount} params"
            );
        }

        return [$out, array_values($params)];
    }

    /**
     * Low-level helper: run a SQL statement with PDO-style `?` placeholders
     * on an amphp executor (connection/transaction). Returns the
     * PostgresResult. Callers iterate via `->fetchRow()` / foreach.
     *
     * @param list<mixed> $params
     */
    public static function exec(PostgresExecutor $conn, string $sql, array $params = []): PostgresResult
    {
        [$translated, $ps] = self::translate($sql, $params);
        return $conn->execute($translated, $ps);
    }

    /**
     * Execute a statement and return all rows as a list of assoc arrays.
     *
     * @param list<mixed> $params
     * @return list<array<string, mixed>>
     */
    public static function fetchAll(PostgresExecutor $conn, string $sql, array $params = []): array
    {
        $result = self::exec($conn, $sql, $params);
        $rows = [];
        foreach ($result as $row) {
            $rows[] = $row;
        }
        return $rows;
    }

    /**
     * Fetch exactly one row, or null if none.
     *
     * @param list<mixed> $params
     * @return array<string, mixed>|null
     */
    public static function fetchOne(PostgresExecutor $conn, string $sql, array $params = []): ?array
    {
        $result = self::exec($conn, $sql, $params);
        return $result->fetchRow();
    }

    /**
     * Fetch the first column of the first row, or null if none.
     *
     * @param list<mixed> $params
     */
    public static function fetchColumn(PostgresExecutor $conn, string $sql, array $params = []): mixed
    {
        $row = self::fetchOne($conn, $sql, $params);
        if ($row === null) {
            return null;
        }
        return reset($row);
    }

    // ========================================================================
    // Search
    // ========================================================================

    public static function search(
        PostgresExecutor $conn,
        string $table,
        string|array $column,
        string $query,
        int $limit = 50,
        string $lang = 'english',
        bool $highlight = false,
    ): array {
        SyncUtils::validateIdentifier($table);
        $columns = is_array($column) ? $column : [$column];
        foreach ($columns as $c) {
            SyncUtils::validateIdentifier($c);
        }
        $tsvector = implode(" || ' ' || ", array_map(fn($c) => "coalesce({$c}, '')", $columns));
        $tsv = "to_tsvector(?, {$tsvector})";
        $tsq = "plainto_tsquery(?, ?)";
        if ($highlight) {
            $fields = "*, ts_rank({$tsv}, {$tsq}) AS _score, ts_headline(?, {$tsvector}, {$tsq}, 'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight";
            $sql = "SELECT {$fields} FROM {$table} WHERE {$tsv} @@ {$tsq} ORDER BY _score DESC LIMIT ?";
            return self::fetchAll(
                $conn,
                $sql,
                [$lang, $lang, $query, $lang, $lang, $query, $lang, $lang, $query, $limit]
            );
        }
        $fields = "*, ts_rank({$tsv}, {$tsq}) AS _score";
        $sql = "SELECT {$fields} FROM {$table} WHERE {$tsv} @@ {$tsq} ORDER BY _score DESC LIMIT ?";
        return self::fetchAll($conn, $sql, [$lang, $lang, $query, $lang, $lang, $query, $limit]);
    }

    public static function searchFuzzy(
        PostgresExecutor $conn,
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        float $threshold = 0.3,
    ): array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        return self::fetchAll($conn, "
            SELECT *, similarity({$column}, ?) AS _score
            FROM {$table}
            WHERE similarity({$column}, ?) > ?
            ORDER BY _score DESC
            LIMIT ?
        ", [$query, $query, $threshold, $limit]);
    }

    public static function searchPhonetic(
        PostgresExecutor $conn,
        string $table,
        string $column,
        string $query,
        int $limit = 50,
    ): array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        return self::fetchAll($conn, "
            SELECT *, similarity({$column}, ?) AS _score
            FROM {$table}
            WHERE soundex({$column}) = soundex(?)
            ORDER BY _score DESC, {$column}
            LIMIT ?
        ", [$query, $query, $limit]);
    }

    public static function similar(
        PostgresExecutor $conn,
        string $table,
        string $column,
        array $vector,
        int $limit = 10,
    ): array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        $vectorLiteral = '[' . implode(',', $vector) . ']';
        return self::fetchAll($conn, "
            SELECT *, ({$column} <=> ?::vector) AS _score
            FROM {$table}
            ORDER BY _score
            LIMIT ?
        ", [$vectorLiteral, $limit]);
    }

    public static function suggest(
        PostgresExecutor $conn,
        string $table,
        string $column,
        string $prefix,
        int $limit = 10,
    ): array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        return self::fetchAll($conn, "
            SELECT *, similarity({$column}, ?) AS _score
            FROM {$table}
            WHERE {$column} ILIKE ?
            ORDER BY _score DESC, {$column}
            LIMIT ?
        ", [$prefix, $prefix . '%', $limit]);
    }

    public static function facets(
        PostgresExecutor $conn,
        string $table,
        string $column,
        int $limit = 50,
        ?string $query = null,
        string|array|null $queryColumn = null,
        string $lang = 'english',
    ): array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        if ($query !== null && $queryColumn !== null) {
            $columns = is_array($queryColumn) ? $queryColumn : [$queryColumn];
            foreach ($columns as $c) {
                SyncUtils::validateIdentifier($c);
            }
            $tsvector = implode(" || ' ' || ", array_map(fn($c) => "coalesce({$c}, '')", $columns));
            $sql = "SELECT {$column} AS value, COUNT(*) AS count FROM {$table} WHERE to_tsvector(?, {$tsvector}) @@ plainto_tsquery(?, ?) GROUP BY {$column} ORDER BY count DESC, {$column} LIMIT ?";
            return self::fetchAll($conn, $sql, [$lang, $lang, $query, $limit]);
        }
        return self::fetchAll(
            $conn,
            "SELECT {$column} AS value, COUNT(*) AS count FROM {$table} GROUP BY {$column} ORDER BY count DESC, {$column} LIMIT ?",
            [$limit]
        );
    }

    public static function aggregate(
        PostgresExecutor $conn,
        string $table,
        string $column,
        string $func,
        ?string $groupBy = null,
        int $limit = 50,
    ): array {
        $allowed = ['count', 'sum', 'avg', 'min', 'max'];
        if (!in_array(strtolower($func), $allowed, true)) {
            throw new \InvalidArgumentException("Invalid aggregate function: {$func}. Must be one of: " . implode(', ', $allowed));
        }
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        $funcUpper = strtoupper($func);
        $expr = $funcUpper === 'COUNT' ? 'COUNT(*)' : "{$funcUpper}({$column})";
        if ($groupBy !== null) {
            SyncUtils::validateIdentifier($groupBy);
            return self::fetchAll(
                $conn,
                "SELECT {$groupBy}, {$expr} AS value FROM {$table} GROUP BY {$groupBy} ORDER BY value DESC LIMIT ?",
                [$limit]
            );
        }
        return self::fetchAll($conn, "SELECT {$expr} AS value FROM {$table}", []);
    }

    public static function createSearchConfig(
        PostgresExecutor $conn,
        string $name,
        string $copyFrom = 'english',
    ): void {
        SyncUtils::validateIdentifier($name);
        SyncUtils::validateIdentifier($copyFrom);
        $existing = self::fetchColumn($conn, "SELECT 1 FROM pg_ts_config WHERE cfgname = ?", [$name]);
        if ($existing !== null) {
            return;
        }
        $conn->query("CREATE TEXT SEARCH CONFIGURATION {$name} (COPY = {$copyFrom})");
    }

    // ========================================================================
    // Pub/Sub & Queue
    // ========================================================================

    public static function publish(PostgresExecutor $conn, string $channel, string $message): void
    {
        // Defence-in-depth: the pg_notify call below is parameterised,
        // so the channel name is passed as a value (not an identifier)
        // and is not an injection surface here. But the sync path and
        // the other 6 language wrappers all validate anyway for
        // cross-wrapper consistency and belt-and-braces — this mirrors
        // that convention.
        SyncUtils::validateIdentifier($channel);
        self::exec($conn, "SELECT pg_notify(?, ?)", [$channel, $message]);
    }

    /**
     * Subscribe to a LISTEN channel. Iterates the amphp listener, calling
     * $callback(string $channel, string $payload) for each notification.
     * Runs until the listener is unlistened or the connection is closed.
     *
     * Requires a full PostgresConnection (not a bare PostgresExecutor) —
     * transactions cannot LISTEN.
     */
    public static function subscribe(PostgresConnection $conn, string $channel, callable $callback): void
    {
        SyncUtils::validateIdentifier($channel);
        $listener = $conn->listen($channel);
        foreach ($listener as $notification) {
            $callback($notification->channel, $notification->payload);
        }
    }

    public static function enqueue(PostgresExecutor $conn, string $queueTable, array $payload): void
    {
        SyncUtils::validateIdentifier($queueTable);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$queueTable} (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ");
        self::exec($conn, "INSERT INTO {$queueTable} (payload) VALUES (?)", [json_encode($payload)]);
    }

    public static function dequeue(PostgresExecutor $conn, string $queueTable): ?array
    {
        SyncUtils::validateIdentifier($queueTable);
        $row = self::fetchOne($conn, "
            DELETE FROM {$queueTable}
            WHERE id = (
                SELECT id FROM {$queueTable}
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING payload
        ");
        if ($row === null) {
            return null;
        }
        $value = $row['payload'] ?? reset($row);
        return is_array($value) ? $value : json_decode($value, true);
    }

    // ========================================================================
    // Counters
    // ========================================================================

    public static function incr(
        PostgresExecutor $conn,
        string $table,
        string $key,
        int $amount = 1,
    ): int {
        SyncUtils::validateIdentifier($table);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$table} (
                key TEXT PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 0
            )
        ");
        $value = self::fetchColumn($conn, "
            INSERT INTO {$table} (key, value) VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = {$table}.value + ?
            RETURNING value
        ", [$key, $amount, $amount]);
        return (int) $value;
    }

    public static function getCounter(
        PostgresExecutor $conn,
        string $table,
        string $key,
    ): int {
        SyncUtils::validateIdentifier($table);
        $value = self::fetchColumn($conn, "SELECT value FROM {$table} WHERE key = ?", [$key]);
        return $value !== null ? (int) $value : 0;
    }

    // ========================================================================
    // Sorted Sets
    // ========================================================================

    public static function zadd(
        PostgresExecutor $conn,
        string $table,
        string $member,
        float $score,
    ): void {
        SyncUtils::validateIdentifier($table);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$table} (
                member TEXT PRIMARY KEY,
                score DOUBLE PRECISION NOT NULL
            )
        ");
        self::exec($conn, "
            INSERT INTO {$table} (member, score) VALUES (?, ?)
            ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score
        ", [$member, $score]);
    }

    public static function zincrby(
        PostgresExecutor $conn,
        string $table,
        string $member,
        float $amount = 1,
    ): float {
        SyncUtils::validateIdentifier($table);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$table} (
                member TEXT PRIMARY KEY,
                score DOUBLE PRECISION NOT NULL
            )
        ");
        $value = self::fetchColumn($conn, "
            INSERT INTO {$table} (member, score) VALUES (?, ?)
            ON CONFLICT (member) DO UPDATE SET score = {$table}.score + ?
            RETURNING score
        ", [$member, $amount, $amount]);
        return (float) $value;
    }

    public static function zrange(
        PostgresExecutor $conn,
        string $table,
        int $start = 0,
        int $stop = 10,
        bool $desc = true,
    ): array {
        SyncUtils::validateIdentifier($table);
        $order = $desc ? 'DESC' : 'ASC';
        $limit = $stop - $start;
        $rows = self::fetchAll($conn, "
            SELECT member, score FROM {$table}
            ORDER BY score {$order}
            LIMIT ? OFFSET ?
        ", [$limit, $start]);
        // Sync returns FETCH_NUM; mirror the shape — list of [member, score].
        $out = [];
        foreach ($rows as $r) {
            $out[] = [$r['member'], $r['score']];
        }
        return $out;
    }

    public static function zrank(
        PostgresExecutor $conn,
        string $table,
        string $member,
        bool $desc = true,
    ): ?int {
        SyncUtils::validateIdentifier($table);
        $order = $desc ? 'DESC' : 'ASC';
        $value = self::fetchColumn($conn, "
            SELECT rank FROM (
                SELECT member, ROW_NUMBER() OVER (ORDER BY score {$order}) - 1 AS rank
                FROM {$table}
            ) ranked
            WHERE member = ?
        ", [$member]);
        return $value !== null ? (int) $value : null;
    }

    public static function zscore(
        PostgresExecutor $conn,
        string $table,
        string $member,
    ): ?float {
        SyncUtils::validateIdentifier($table);
        $value = self::fetchColumn($conn, "SELECT score FROM {$table} WHERE member = ?", [$member]);
        return $value !== null ? (float) $value : null;
    }

    public static function zrem(
        PostgresExecutor $conn,
        string $table,
        string $member,
    ): bool {
        SyncUtils::validateIdentifier($table);
        $result = self::exec($conn, "DELETE FROM {$table} WHERE member = ?", [$member]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    // ========================================================================
    // Geo
    // ========================================================================

    public static function geoadd(
        PostgresExecutor $conn,
        string $table,
        string $nameColumn,
        string $geomColumn,
        string $name,
        float $lon,
        float $lat,
    ): void {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($nameColumn);
        SyncUtils::validateIdentifier($geomColumn);
        $conn->query("CREATE EXTENSION IF NOT EXISTS postgis");
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$table} (
                id BIGSERIAL PRIMARY KEY,
                {$nameColumn} TEXT NOT NULL,
                {$geomColumn} GEOMETRY(Point, 4326) NOT NULL
            )
        ");
        self::exec($conn, "
            INSERT INTO {$table} ({$nameColumn}, {$geomColumn})
            VALUES (?, ST_SetSRID(ST_MakePoint(?, ?), 4326))
        ", [$name, $lon, $lat]);
    }

    public static function georadius(
        PostgresExecutor $conn,
        string $table,
        string $geomColumn,
        float $lon,
        float $lat,
        float $radiusMeters,
        int $limit = 50,
    ): array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($geomColumn);
        return self::fetchAll($conn, "
            SELECT *, ST_Distance(
                {$geomColumn}::geography,
                ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography
            ) AS distance_m
            FROM {$table}
            WHERE ST_DWithin(
                {$geomColumn}::geography,
                ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography,
                ?
            )
            ORDER BY distance_m
            LIMIT ?
        ", [$lon, $lat, $lon, $lat, $radiusMeters, $limit]);
    }

    public static function geodist(
        PostgresExecutor $conn,
        string $table,
        string $geomColumn,
        string $nameColumn,
        string $nameA,
        string $nameB,
    ): ?float {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($geomColumn);
        SyncUtils::validateIdentifier($nameColumn);
        $value = self::fetchColumn($conn, "
            SELECT ST_Distance(a.{$geomColumn}::geography, b.{$geomColumn}::geography)
            FROM {$table} a, {$table} b
            WHERE a.{$nameColumn} = ? AND b.{$nameColumn} = ?
        ", [$nameA, $nameB]);
        return $value !== null ? (float) $value : null;
    }

    // ========================================================================
    // Hash
    // ========================================================================

    public static function hset(
        PostgresExecutor $conn,
        string $table,
        string $key,
        string $field,
        mixed $value,
    ): void {
        SyncUtils::validateIdentifier($table);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$table} (
                key TEXT PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        ");
        $json = json_encode($value);
        // Explicit ::text casts needed: amphp/postgres uses libpq's
        // prepared-statement path which asks the server to infer parameter
        // types. jsonb_build_object is polymorphic, so the server can't
        // infer that $2 is text — add the cast.
        self::exec($conn, "
            INSERT INTO {$table} (key, data) VALUES (?, jsonb_build_object(?::text, ?::jsonb))
            ON CONFLICT (key) DO UPDATE SET data = {$table}.data || jsonb_build_object(?::text, ?::jsonb)
        ", [$key, $field, $json, $field, $json]);
    }

    public static function hget(
        PostgresExecutor $conn,
        string $table,
        string $key,
        string $field,
    ): mixed {
        SyncUtils::validateIdentifier($table);
        $value = self::fetchColumn($conn, "SELECT data->>? FROM {$table} WHERE key = ?", [$field, $key]);
        if ($value === null) {
            return null;
        }
        $decoded = json_decode($value, true);
        return json_last_error() === JSON_ERROR_NONE ? $decoded : $value;
    }

    public static function hgetall(
        PostgresExecutor $conn,
        string $table,
        string $key,
    ): array {
        SyncUtils::validateIdentifier($table);
        $value = self::fetchColumn($conn, "SELECT data FROM {$table} WHERE key = ?", [$key]);
        if ($value === null) {
            return [];
        }
        // amphp returns jsonb columns as text strings — decode to match sync
        // behavior (sync json_decodes the PDO-returned string).
        return is_string($value) ? (json_decode($value, true) ?? []) : (array) $value;
    }

    public static function hdel(
        PostgresExecutor $conn,
        string $table,
        string $key,
        string $field,
    ): bool {
        SyncUtils::validateIdentifier($table);
        $row = self::fetchOne($conn, "SELECT data ? ? AS existed FROM {$table} WHERE key = ?", [$field, $key]);
        if ($row === null || !$row['existed']) {
            return false;
        }
        self::exec($conn, "UPDATE {$table} SET data = data - ? WHERE key = ?", [$field, $key]);
        return true;
    }

    // ========================================================================
    // Misc
    // ========================================================================

    public static function countDistinct(
        PostgresExecutor $conn,
        string $table,
        string $column,
    ): int {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        $value = self::fetchColumn($conn, "SELECT COUNT(DISTINCT {$column}) FROM {$table}");
        return (int) $value;
    }

    public static function script(PostgresExecutor $conn, string $luaCode, mixed ...$args): ?string
    {
        $conn->query("CREATE EXTENSION IF NOT EXISTS pllua");
        $funcName = "_gl_lua_" . bin2hex(random_bytes(4));
        if (empty($args)) {
            $conn->query(
                "CREATE OR REPLACE FUNCTION pg_temp.{$funcName}() RETURNS text LANGUAGE pllua AS \$pllua\$ {$luaCode} \$pllua\$"
            );
            $row = self::fetchOne($conn, "SELECT pg_temp.{$funcName}()");
        } else {
            $params = implode(", ", array_map(fn($i) => "p" . ($i + 1) . " text", range(0, count($args) - 1)));
            $conn->query(
                "CREATE OR REPLACE FUNCTION pg_temp.{$funcName}({$params}) RETURNS text LANGUAGE pllua AS \$pllua\$ {$luaCode} \$pllua\$"
            );
            $placeholders = implode(", ", array_fill(0, count($args), '?'));
            $row = self::fetchOne(
                $conn,
                "SELECT pg_temp.{$funcName}({$placeholders})",
                array_map('strval', $args)
            );
        }
        if ($row === null) {
            return null;
        }
        $first = reset($row);
        return $first === null ? null : (string) $first;
    }

    // ========================================================================
    // Streams
    // ========================================================================

    public static function streamAdd(PostgresExecutor $conn, string $stream, array $payload): int
    {
        SyncUtils::validateIdentifier($stream);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$stream} (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ");
        $id = self::fetchColumn(
            $conn,
            "INSERT INTO {$stream} (payload) VALUES (?) RETURNING id",
            [json_encode($payload)]
        );
        return (int) $id;
    }

    public static function streamCreateGroup(PostgresExecutor $conn, string $stream, string $group): void
    {
        SyncUtils::validateIdentifier($stream);
        $groupsTable = $stream . '_groups';
        $pendingTable = $stream . '_pending';
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$groupsTable} (
                group_name TEXT PRIMARY KEY,
                last_id BIGINT NOT NULL DEFAULT 0
            )
        ");
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$pendingTable} (
                message_id BIGINT NOT NULL,
                group_name TEXT NOT NULL,
                consumer TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (group_name, message_id)
            )
        ");
        self::exec(
            $conn,
            "INSERT INTO {$groupsTable} (group_name, last_id) VALUES (?, 0) ON CONFLICT (group_name) DO NOTHING",
            [$group]
        );
    }

    /**
     * Read pending messages from a stream. Requires a PostgresConnection so
     * it can open a transaction (FOR UPDATE + UPDATE group state + INSERT
     * pending must be atomic).
     */
    public static function streamRead(
        PostgresConnection $conn,
        string $stream,
        string $group,
        string $consumer,
        int $count = 1,
    ): array {
        SyncUtils::validateIdentifier($stream);
        $groupsTable = $stream . '_groups';
        $pendingTable = $stream . '_pending';

        $tx = $conn->beginTransaction();
        try {
            $lastId = self::fetchColumn(
                $tx,
                "SELECT last_id FROM {$groupsTable} WHERE group_name = ? FOR UPDATE",
                [$group]
            );
            if ($lastId === null) {
                $tx->commit();
                return [];
            }

            $messages = self::fetchAll($tx, "
                SELECT id, payload, created_at FROM {$stream}
                WHERE id > ?
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            ", [(int) $lastId, $count]);

            if (empty($messages)) {
                $tx->commit();
                return [];
            }

            $maxId = 0;
            foreach ($messages as &$msg) {
                if (is_string($msg['payload'])) {
                    $msg['payload'] = json_decode($msg['payload'], true);
                }
                $id = (int) $msg['id'];
                if ($id > $maxId) {
                    $maxId = $id;
                }
                self::exec($tx, "
                    INSERT INTO {$pendingTable} (message_id, group_name, consumer)
                    VALUES (?, ?, ?)
                    ON CONFLICT (group_name, message_id) DO NOTHING
                ", [$id, $group, $consumer]);
            }
            unset($msg);

            self::exec(
                $tx,
                "UPDATE {$groupsTable} SET last_id = ? WHERE group_name = ?",
                [$maxId, $group]
            );
            $tx->commit();
            return $messages;
        } catch (\Throwable $e) {
            if ($tx->isActive()) {
                $tx->rollback();
            }
            throw $e;
        }
    }

    public static function streamAck(
        PostgresExecutor $conn,
        string $stream,
        string $group,
        int $messageId,
    ): bool {
        SyncUtils::validateIdentifier($stream);
        $pendingTable = $stream . '_pending';
        $result = self::exec(
            $conn,
            "DELETE FROM {$pendingTable} WHERE group_name = ? AND message_id = ?",
            [$group, $messageId]
        );
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function streamClaim(
        PostgresExecutor $conn,
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
    ): array {
        SyncUtils::validateIdentifier($stream);
        $pendingTable = $stream . '_pending';
        $claimedRows = self::fetchAll($conn, "
            UPDATE {$pendingTable}
            SET consumer = ?, assigned_at = NOW()
            WHERE group_name = ?
            AND assigned_at < NOW() - (? || ' milliseconds')::interval
            RETURNING message_id
        ", [$consumer, $group, $minIdleMs]);
        $claimed = array_map(fn($r) => (int) $r['message_id'], $claimedRows);
        if (empty($claimed)) {
            return [];
        }
        $placeholders = implode(', ', array_fill(0, count($claimed), '?'));
        $messages = self::fetchAll($conn, "
            SELECT id, payload, created_at FROM {$stream}
            WHERE id IN ({$placeholders})
            ORDER BY id
        ", $claimed);
        foreach ($messages as &$msg) {
            if (is_string($msg['payload'])) {
                $msg['payload'] = json_decode($msg['payload'], true);
            }
        }
        unset($msg);
        return $messages;
    }

    // ========================================================================
    // Percolate
    // ========================================================================

    public static function percolateAdd(
        PostgresExecutor $conn,
        string $name,
        string $queryId,
        string $query,
        string $lang = 'english',
        ?array $metadata = null,
    ): void {
        SyncUtils::validateIdentifier($name);
        $conn->query("
            CREATE TABLE IF NOT EXISTS {$name} (
                query_id TEXT PRIMARY KEY,
                query_text TEXT NOT NULL,
                tsquery TSQUERY NOT NULL,
                lang TEXT NOT NULL DEFAULT 'english',
                metadata JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ");
        $conn->query("CREATE INDEX IF NOT EXISTS {$name}_tsq_idx ON {$name} USING GIST (tsquery)");
        self::exec($conn, "
            INSERT INTO {$name} (query_id, query_text, tsquery, lang, metadata)
            VALUES (?, ?, plainto_tsquery(?, ?), ?, ?)
            ON CONFLICT (query_id) DO UPDATE SET
                query_text = EXCLUDED.query_text,
                tsquery = EXCLUDED.tsquery,
                lang = EXCLUDED.lang,
                metadata = EXCLUDED.metadata
        ", [
            $queryId,
            $query,
            $lang,
            $query,
            $lang,
            $metadata !== null ? json_encode($metadata) : null,
        ]);
    }

    public static function percolate(
        PostgresExecutor $conn,
        string $name,
        string $text,
        int $limit = 50,
        string $lang = 'english',
    ): array {
        SyncUtils::validateIdentifier($name);
        return self::fetchAll($conn, "
            SELECT query_id, query_text, metadata,
                ts_rank(to_tsvector(?, ?), tsquery) AS _score
            FROM {$name}
            WHERE to_tsvector(?, ?) @@ tsquery
            ORDER BY _score DESC
            LIMIT ?
        ", [$lang, $text, $lang, $text, $limit]);
    }

    public static function percolateDelete(
        PostgresExecutor $conn,
        string $name,
        string $queryId,
    ): bool {
        SyncUtils::validateIdentifier($name);
        $result = self::exec(
            $conn,
            "DELETE FROM {$name} WHERE query_id = ? RETURNING query_id",
            [$queryId]
        );
        return ($result->getRowCount() ?? 0) > 0;
    }

    // ========================================================================
    // Debug
    // ========================================================================

    public static function analyze(
        PostgresExecutor $conn,
        string $text,
        string $lang = 'english',
    ): array {
        return self::fetchAll(
            $conn,
            "SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug(?, ?)",
            [$lang, $text]
        );
    }

    public static function explainScore(
        PostgresExecutor $conn,
        string $table,
        string $column,
        string $query,
        string $idColumn,
        mixed $idValue,
        string $lang = 'english',
    ): ?array {
        SyncUtils::validateIdentifier($table);
        SyncUtils::validateIdentifier($column);
        SyncUtils::validateIdentifier($idColumn);
        $sql = "SELECT {$column} AS document_text, to_tsvector(?, {$column})::text AS document_tokens, "
            . "plainto_tsquery(?, ?)::text AS query_tokens, "
            . "to_tsvector(?, {$column}) @@ plainto_tsquery(?, ?) AS matches, "
            . "ts_rank(to_tsvector(?, {$column}), plainto_tsquery(?, ?)) AS score, "
            . "ts_headline(?, {$column}, plainto_tsquery(?, ?), 'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline "
            . "FROM {$table} WHERE {$idColumn} = ?";
        return self::fetchOne($conn, $sql, [$lang, $lang, $query, $lang, $lang, $query, $lang, $lang, $query, $lang, $lang, $query, $idValue]);
    }

    // ========================================================================
    // Document Store
    // ========================================================================

    private static function ensureCollection(
        PostgresExecutor $conn,
        string $collection,
        bool $unlogged = false,
    ): void {
        $prefix = $unlogged ? 'CREATE UNLOGGED TABLE' : 'CREATE TABLE';
        $conn->query(
            "{$prefix} IF NOT EXISTS {$collection} ("
            . "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), "
            . "data JSONB NOT NULL, "
            . "created_at TIMESTAMPTZ DEFAULT NOW())"
        );
    }

    public static function docCreateCollection(
        PostgresExecutor $conn,
        string $collection,
        bool $unlogged = false,
    ): void {
        SyncUtils::validateIdentifier($collection);
        self::ensureCollection($conn, $collection, $unlogged);
    }

    public static function docInsert(
        PostgresExecutor $conn,
        string $collection,
        array $document,
    ): array {
        SyncUtils::validateIdentifier($collection);
        self::ensureCollection($conn, $collection);
        return self::fetchOne(
            $conn,
            "INSERT INTO {$collection} (data) VALUES (?::jsonb) RETURNING _id, data, created_at",
            [json_encode($document)]
        );
    }

    public static function docInsertMany(
        PostgresExecutor $conn,
        string $collection,
        array $documents,
    ): array {
        SyncUtils::validateIdentifier($collection);
        if (empty($documents)) {
            return [];
        }
        self::ensureCollection($conn, $collection);
        $placeholders = implode(', ', array_map(fn() => '(?::jsonb)', $documents));
        $params = array_map(fn($d) => json_encode($d), $documents);
        return self::fetchAll(
            $conn,
            "INSERT INTO {$collection} (data) VALUES {$placeholders} RETURNING _id, data, created_at",
            $params
        );
    }

    public static function docFind(
        PostgresExecutor $conn,
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$collection}";
        if ($clause !== '') {
            $sql .= " WHERE {$clause}";
        }
        if ($sort !== null && count($sort) > 0) {
            $clauses = [];
            foreach ($sort as $key => $dir) {
                if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                    throw new \InvalidArgumentException("Invalid sort key: {$key}");
                }
                $clauses[] = "data->>'{$key}' " . ($dir === -1 ? 'DESC' : 'ASC');
            }
            $sql .= " ORDER BY " . implode(', ', $clauses);
        }
        if ($limit !== null) {
            $params[] = $limit;
            $sql .= " LIMIT ?";
        }
        if ($skip !== null) {
            $params[] = $skip;
            $sql .= " OFFSET ?";
        }
        return self::fetchAll($conn, $sql, $params);
    }

    /**
     * Async cursor over a collection — returns an Amp\Pipeline<array> you
     * can iterate with `foreach`. The underlying implementation uses a
     * SQL cursor inside a transaction, so it requires a full
     * PostgresConnection (not a bare executor).
     *
     * @return \Amp\Pipeline\ConcurrentIterator<array<string, mixed>>
     */
    public static function docFindCursor(
        PostgresConnection $conn,
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
        int $batchSize = 100,
    ): \Amp\Pipeline\ConcurrentIterator {
        SyncUtils::validateIdentifier($collection);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$collection}";
        if ($clause !== '') {
            $sql .= " WHERE {$clause}";
        }
        if ($sort !== null && count($sort) > 0) {
            $clauses = [];
            foreach ($sort as $key => $dir) {
                if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                    throw new \InvalidArgumentException("Invalid sort key: {$key}");
                }
                $clauses[] = "data->>'{$key}' " . ($dir === -1 ? 'DESC' : 'ASC');
            }
            $sql .= " ORDER BY " . implode(', ', $clauses);
        }
        if ($limit !== null) {
            $params[] = $limit;
            $sql .= " LIMIT ?";
        }
        if ($skip !== null) {
            $params[] = $skip;
            $sql .= " OFFSET ?";
        }

        $cursorName = 'gl_cursor_' . bin2hex(random_bytes(4));
        // Use a buffered queue. pushAsync returns a Future for each push,
        // which we discard — the queue buffers pending values and the
        // consumer drains them via iterate(). Buffer size matches batchSize
        // so a whole batch can be pushed without producer suspending.
        $queue = new Queue($batchSize + 1);

        \Amp\async(static function () use ($conn, $sql, $params, $cursorName, $batchSize, $queue): void {
            $tx = $conn->beginTransaction();
            try {
                self::exec($tx, "DECLARE {$cursorName} CURSOR FOR {$sql}", $params);
                while (true) {
                    // Tag each FETCH with the `goldlapel:skip` annotation so
                    // the Rust proxy doesn't put it through rewrite + result-
                    // cache (cursor FETCH is stateful and must not be
                    // replayed from a cached batch).
                    $result = $tx->query("/* goldlapel:skip */ FETCH {$batchSize} FROM {$cursorName}");
                    $count = 0;
                    foreach ($result as $row) {
                        $queue->pushAsync($row);
                        $count++;
                    }
                    if ($count === 0) {
                        break;
                    }
                    if ($count < $batchSize) {
                        // Partial batch means the cursor is exhausted;
                        // skip the extra FETCH that would return 0 rows.
                        break;
                    }
                }
                $tx->query("CLOSE {$cursorName}");
                $tx->commit();
                $queue->complete();
            } catch (\Throwable $e) {
                if ($tx->isActive()) {
                    try {
                        $tx->rollback();
                    } catch (\Throwable $inner) {
                        // ignore
                    }
                }
                $queue->error($e);
            }
        });

        return $queue->iterate();
    }

    public static function docFindOne(
        PostgresExecutor $conn,
        string $collection,
        ?array $filter = null,
    ): ?array {
        SyncUtils::validateIdentifier($collection);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$collection}";
        if ($clause !== '') {
            $sql .= " WHERE {$clause}";
        }
        $sql .= " LIMIT 1";
        return self::fetchOne($conn, $sql, $params);
    }

    public static function docUpdate(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        array $update,
    ): int {
        SyncUtils::validateIdentifier($collection);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        [$updateExpr, $updateParams] = SyncUtils::buildUpdate($update);
        $where = $clause !== '' ? $clause : 'TRUE';
        $allParams = [...$updateParams, ...$filterParams];
        $result = self::exec(
            $conn,
            "UPDATE {$collection} SET data = {$updateExpr} WHERE {$where}",
            $allParams
        );
        return $result->getRowCount() ?? 0;
    }

    public static function docUpdateOne(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        array $update,
    ): int {
        SyncUtils::validateIdentifier($collection);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        [$updateExpr, $updateParams] = SyncUtils::buildUpdate($update);
        $where = $clause !== '' ? $clause : 'TRUE';
        $allParams = [...$filterParams, ...$updateParams];
        $result = self::exec($conn,
            "WITH target AS ("
            . "SELECT _id FROM {$collection} WHERE {$where} LIMIT 1"
            . ") UPDATE {$collection} SET data = {$updateExpr} "
            . "FROM target WHERE {$collection}._id = target._id",
            $allParams
        );
        return $result->getRowCount() ?? 0;
    }

    public static function docDelete(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
    ): int {
        SyncUtils::validateIdentifier($collection);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $result = self::exec($conn, "DELETE FROM {$collection} WHERE {$where}", $params);
        return $result->getRowCount() ?? 0;
    }

    public static function docDeleteOne(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
    ): int {
        SyncUtils::validateIdentifier($collection);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $result = self::exec($conn,
            "WITH target AS ("
            . "SELECT _id FROM {$collection} WHERE {$where} LIMIT 1"
            . ") DELETE FROM {$collection} USING target WHERE {$collection}._id = target._id",
            $params
        );
        return $result->getRowCount() ?? 0;
    }

    public static function docCount(
        PostgresExecutor $conn,
        string $collection,
        ?array $filter = null,
    ): int {
        SyncUtils::validateIdentifier($collection);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT COUNT(*) FROM {$collection}";
        if ($clause !== '') {
            $sql .= " WHERE {$clause}";
        }
        return (int) self::fetchColumn($conn, $sql, $params);
    }

    public static function docFindOneAndUpdate(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        array $update,
    ): ?array {
        SyncUtils::validateIdentifier($collection);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        [$updateExpr, $updateParams] = SyncUtils::buildUpdate($update);
        $cteWhere = $clause !== '' ? " WHERE {$clause}" : '';
        $sql = "WITH target AS ("
            . "SELECT _id FROM {$collection}{$cteWhere} LIMIT 1"
            . ") UPDATE {$collection} SET data = {$updateExpr} FROM target "
            . "WHERE {$collection}._id = target._id "
            . "RETURNING {$collection}._id, {$collection}.data, {$collection}.created_at";
        $allParams = [...$filterParams, ...$updateParams];
        return self::fetchOne($conn, $sql, $allParams);
    }

    public static function docFindOneAndDelete(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
    ): ?array {
        SyncUtils::validateIdentifier($collection);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        $cteWhere = $clause !== '' ? " WHERE {$clause}" : '';
        $sql = "WITH target AS ("
            . "SELECT _id FROM {$collection}{$cteWhere} LIMIT 1"
            . ") DELETE FROM {$collection} USING target "
            . "WHERE {$collection}._id = target._id "
            . "RETURNING {$collection}._id, {$collection}.data, {$collection}.created_at";
        return self::fetchOne($conn, $sql, $filterParams);
    }

    public static function docDistinct(
        PostgresExecutor $conn,
        string $collection,
        string $field,
        ?array $filter = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        $fieldExpr = SyncUtils::fieldPath($field);
        $sql = "SELECT DISTINCT {$fieldExpr} FROM {$collection}";
        $params = [];
        $whereParts = ["{$fieldExpr} IS NOT NULL"];
        [$filterClause, $filterParams] = SyncUtils::buildFilter($filter);
        if ($filterClause !== '') {
            $whereParts[] = $filterClause;
            array_push($params, ...$filterParams);
        }
        $sql .= " WHERE " . implode(' AND ', $whereParts);
        $rows = self::fetchAll($conn, $sql, $params);
        $out = [];
        foreach ($rows as $r) {
            $out[] = reset($r);
        }
        return $out;
    }

    /**
     * docAggregate is a near-verbatim port of the sync pipeline builder —
     * reuses SyncUtils::buildFilter/resolveFieldRef so we keep one source of
     * truth for all the $match/$group/$project/$unwind/$lookup behavior.
     */
    public static function docAggregate(
        PostgresExecutor $conn,
        string $collection,
        array $pipeline,
    ): array {
        SyncUtils::validateIdentifier($collection);

        if (empty($pipeline)) {
            return [];
        }

        $supportedStages = ['$match', '$group', '$sort', '$limit', '$skip', '$project', '$unwind', '$lookup'];

        $matchStage = null;
        $groupStage = null;
        $sortStage = null;
        $limitValue = null;
        $offsetValue = null;
        $projectStage = null;
        $unwindStage = null;
        $lookupStages = [];

        foreach ($pipeline as $stage) {
            if (!is_array($stage) || count($stage) !== 1) {
                throw new \InvalidArgumentException("Each pipeline stage must be an associative array with exactly one key");
            }
            $stageKey = array_key_first($stage);
            $stageValue = $stage[$stageKey];
            if (!in_array($stageKey, $supportedStages, true)) {
                throw new \InvalidArgumentException("Unsupported pipeline stage: {$stageKey}");
            }
            switch ($stageKey) {
                case '$match': $matchStage = $stageValue; break;
                case '$group': $groupStage = $stageValue; break;
                case '$sort': $sortStage = $stageValue; break;
                case '$limit': $limitValue = $stageValue; break;
                case '$skip': $offsetValue = $stageValue; break;
                case '$project': $projectStage = $stageValue; break;
                case '$unwind': $unwindStage = $stageValue; break;
                case '$lookup': $lookupStages[] = $stageValue; break;
            }
        }

        if ($limitValue !== null && (!is_int($limitValue) || $limitValue < 0)) {
            throw new \InvalidArgumentException('$limit must be a non-negative integer');
        }
        if ($offsetValue !== null && (!is_int($offsetValue) || $offsetValue < 0)) {
            throw new \InvalidArgumentException('$skip must be a non-negative integer');
        }

        $unwindField = null;
        $unwindAlias = null;
        $unwindMap = [];
        if ($unwindStage !== null) {
            if (is_string($unwindStage)) {
                if (!str_starts_with($unwindStage, '$')) {
                    throw new \InvalidArgumentException('$unwind path must start with $');
                }
                $unwindField = substr($unwindStage, 1);
            } elseif (is_array($unwindStage) && isset($unwindStage['path'])) {
                $path = $unwindStage['path'];
                if (!is_string($path) || !str_starts_with($path, '$')) {
                    throw new \InvalidArgumentException('$unwind path must start with $');
                }
                $unwindField = substr($path, 1);
            } else {
                throw new \InvalidArgumentException('$unwind must be a string or array with path');
            }
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $unwindField)) {
                throw new \InvalidArgumentException("Invalid field name: {$unwindField}");
            }
            $unwindAlias = "_uw_{$unwindField}";
            $unwindMap[$unwindField] = $unwindAlias;
        }

        $lookupSqls = [];
        foreach ($lookupStages as $lk) {
            $from = $lk['from'] ?? null;
            $localField = $lk['localField'] ?? null;
            $foreignField = $lk['foreignField'] ?? null;
            $asField = $lk['as'] ?? null;
            if ($from === null || $localField === null || $foreignField === null || $asField === null) {
                throw new \InvalidArgumentException('$lookup requires from, localField, foreignField, and as');
            }
            SyncUtils::validateIdentifier($from);
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $localField)) {
                throw new \InvalidArgumentException("Invalid field name: {$localField}");
            }
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $foreignField)) {
                throw new \InvalidArgumentException("Invalid field name: {$foreignField}");
            }
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $asField)) {
                throw new \InvalidArgumentException("Invalid field name: {$asField}");
            }
            $lookupSqls[] = "COALESCE((SELECT json_agg({$from}.data) FROM {$from} WHERE {$from}.data->>'{$foreignField}' = {$collection}.data->>'{$localField}'), '[]'::json) AS {$asField}";
        }

        $whereParams = [];
        $where = '';
        if ($matchStage !== null) {
            if (!is_array($matchStage)) {
                throw new \InvalidArgumentException('$match stage value must be an array');
            }
            [$matchClause, $matchParams] = SyncUtils::buildFilter($matchStage);
            if ($matchClause !== '') {
                $where = "WHERE {$matchClause}";
                $whereParams = $matchParams;
            }
        }

        $params = [];
        $selectFields = [];
        $groupBy = '';
        $accumulators = [];

        if ($projectStage !== null) {
            if (!is_array($projectStage)) {
                throw new \InvalidArgumentException('$project stage value must be an array');
            }
            $projectParts = [];
            foreach ($projectStage as $key => $val) {
                if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                    throw new \InvalidArgumentException("Invalid field name: {$key}");
                }
                if ($val === 0 || $val === false) {
                    continue;
                }
                if ($val === 1 || $val === true) {
                    if ($key === '_id') {
                        $projectParts[] = "_id";
                    } elseif ($groupStage !== null && isset($accumulators[$key])) {
                        $projectParts[] = $key;
                    } else {
                        $projectParts[] = SyncUtils::resolveFieldRef('$' . $key, $unwindMap) . " AS {$key}";
                    }
                } elseif (is_string($val) && str_starts_with($val, '$')) {
                    $projectParts[] = SyncUtils::resolveFieldRef($val, $unwindMap) . " AS {$key}";
                } else {
                    throw new \InvalidArgumentException("Unsupported \$project value for {$key}: {$val}");
                }
            }
            if (isset($projectStage['_id']) && ($projectStage['_id'] === 0 || $projectStage['_id'] === false)) {
                // exclude
            } elseif (!isset($projectStage['_id'])) {
                array_unshift($projectParts, '_id');
            }
            $allParts = array_merge($projectParts, $lookupSqls);
            $selectFields = $allParts;
            $sql = "SELECT " . implode(', ', $selectFields) . " FROM {$collection}";
            if ($unwindField !== null) {
                $sql .= " CROSS JOIN jsonb_array_elements_text(data->'{$unwindField}') AS {$unwindAlias}";
            }
            if ($where !== '') {
                $sql .= " {$where}";
                $params = array_merge($params, $whereParams);
            }
            if ($sortStage !== null) {
                if (!is_array($sortStage)) {
                    throw new \InvalidArgumentException('$sort stage value must be an array');
                }
                $sortClauses = [];
                foreach ($sortStage as $key => $dir) {
                    if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                        throw new \InvalidArgumentException("Invalid sort key: {$key}");
                    }
                    $direction = $dir === -1 ? 'DESC' : 'ASC';
                    $sortClauses[] = "{$key} {$direction}";
                }
                $sql .= " ORDER BY " . implode(', ', $sortClauses);
            }
        } elseif ($groupStage !== null) {
            if (!is_array($groupStage) || !array_key_exists('_id', $groupStage)) {
                throw new \InvalidArgumentException('$group stage must contain _id');
            }
            $groupId = $groupStage['_id'];
            if ($groupId === null) {
                // no group key
            } elseif (is_array($groupId)) {
                if (array_is_list($groupId) || empty($groupId)) {
                    throw new \InvalidArgumentException('$group _id object must be a non-empty associative array');
                }
                $objParts = [];
                $groupByCols = [];
                foreach ($groupId as $alias => $ref) {
                    if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $alias)) {
                        throw new \InvalidArgumentException("Invalid _id alias: {$alias}");
                    }
                    if (!is_string($ref) || $ref === '' || $ref[0] !== '$') {
                        throw new \InvalidArgumentException("Invalid field reference in _id: each value must be a \"\$field\" reference");
                    }
                    $resolved = SyncUtils::resolveFieldRef($ref, $unwindMap);
                    $objParts[] = "'{$alias}', {$resolved}";
                    $groupByCols[] = $resolved;
                }
                $selectFields[] = "json_build_object(" . implode(', ', $objParts) . ") AS _id";
                $groupBy = "GROUP BY " . implode(', ', $groupByCols);
            } else {
                if (!is_string($groupId) || $groupId === '' || $groupId[0] !== '$') {
                    throw new \InvalidArgumentException('$group _id must be null, a "$field" reference, or an associative array');
                }
                $resolved = SyncUtils::resolveFieldRef($groupId, $unwindMap);
                $selectFields[] = "{$resolved} AS _id";
                $groupBy = "GROUP BY {$resolved}";
            }

            foreach ($groupStage as $alias => $expr) {
                if ($alias === '_id') {
                    continue;
                }
                if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $alias)) {
                    throw new \InvalidArgumentException("Invalid accumulator alias: {$alias}");
                }
                if (!is_array($expr) || count($expr) !== 1) {
                    throw new \InvalidArgumentException("Accumulator for '{$alias}' must be an associative array with one operator");
                }
                $op = array_key_first($expr);
                $opValue = $expr[$op];
                $accumulators[$alias] = true;
                switch ($op) {
                    case '$sum':
                        if ($opValue === 1) {
                            $selectFields[] = "COUNT(*) AS {$alias}";
                        } else {
                            if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                                throw new \InvalidArgumentException("Invalid field reference in \$sum");
                            }
                            $resolved = SyncUtils::resolveFieldRef($opValue, $unwindMap);
                            $selectFields[] = "SUM(({$resolved})::numeric) AS {$alias}";
                        }
                        break;
                    case '$avg':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$avg");
                        }
                        $resolved = SyncUtils::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "AVG(({$resolved})::numeric) AS {$alias}";
                        break;
                    case '$min':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$min");
                        }
                        $resolved = SyncUtils::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "MIN(({$resolved})::numeric) AS {$alias}";
                        break;
                    case '$max':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$max");
                        }
                        $resolved = SyncUtils::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "MAX(({$resolved})::numeric) AS {$alias}";
                        break;
                    case '$count':
                        $selectFields[] = "COUNT(*) AS {$alias}";
                        break;
                    case '$push':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$push");
                        }
                        $resolved = SyncUtils::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "array_agg({$resolved}) AS {$alias}";
                        break;
                    case '$addToSet':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$addToSet");
                        }
                        $resolved = SyncUtils::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "array_agg(DISTINCT {$resolved}) AS {$alias}";
                        break;
                    default:
                        throw new \InvalidArgumentException("Unsupported accumulator: {$op}");
                }
            }

            $allParts = array_merge($selectFields, $lookupSqls);
            $sql = "SELECT " . implode(', ', $allParts) . " FROM {$collection}";
            if ($unwindField !== null) {
                $sql .= " CROSS JOIN jsonb_array_elements_text(data->'{$unwindField}') AS {$unwindAlias}";
            }
            if ($where !== '') {
                $sql .= " {$where}";
                $params = array_merge($params, $whereParams);
            }
            if ($groupBy !== '') {
                $sql .= " {$groupBy}";
            }
            if ($sortStage !== null) {
                if (!is_array($sortStage)) {
                    throw new \InvalidArgumentException('$sort stage value must be an array');
                }
                $sortClauses = [];
                foreach ($sortStage as $key => $dir) {
                    if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                        throw new \InvalidArgumentException("Invalid sort key: {$key}");
                    }
                    $direction = $dir === -1 ? 'DESC' : 'ASC';
                    if (isset($accumulators[$key])) {
                        $sortClauses[] = "{$key} {$direction}";
                    } else {
                        $sortClauses[] = "data->>'{$key}' {$direction}";
                    }
                }
                $sql .= " ORDER BY " . implode(', ', $sortClauses);
            }
        } else {
            $baseParts = array_merge(['_id', 'data', 'created_at'], $lookupSqls);
            $sql = "SELECT " . implode(', ', $baseParts) . " FROM {$collection}";
            if ($unwindField !== null) {
                $sql .= " CROSS JOIN jsonb_array_elements_text(data->'{$unwindField}') AS {$unwindAlias}";
            }
            if ($where !== '') {
                $sql .= " {$where}";
                $params = array_merge($params, $whereParams);
            }
            if ($sortStage !== null) {
                if (!is_array($sortStage)) {
                    throw new \InvalidArgumentException('$sort stage value must be an array');
                }
                $sortClauses = [];
                foreach ($sortStage as $key => $dir) {
                    if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                        throw new \InvalidArgumentException("Invalid sort key: {$key}");
                    }
                    $direction = $dir === -1 ? 'DESC' : 'ASC';
                    $sortClauses[] = "data->>'{$key}' {$direction}";
                }
                $sql .= " ORDER BY " . implode(', ', $sortClauses);
            }
        }

        if ($limitValue !== null) {
            $sql .= " LIMIT ?";
            $params[] = $limitValue;
        }
        if ($offsetValue !== null) {
            $sql .= " OFFSET ?";
            $params[] = $offsetValue;
        }

        return self::fetchAll($conn, $sql, $params);
    }

    public static function docCreateIndex(
        PostgresExecutor $conn,
        string $collection,
        ?array $keys = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        if ($keys === null || count($keys) === 0) {
            $conn->query(
                "CREATE INDEX IF NOT EXISTS {$collection}_data_gin ON {$collection} USING GIN (data)"
            );
            return;
        }
        foreach ($keys as $key => $dir) {
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                throw new \InvalidArgumentException("Invalid index key: {$key}");
            }
            $order = $dir === -1 ? 'DESC' : 'ASC';
            $safeName = str_replace('.', '_', $key);
            $conn->query(
                "CREATE INDEX IF NOT EXISTS {$collection}_{$safeName}_idx "
                . "ON {$collection} ((data->>'{$key}') {$order})"
            );
        }
    }

    // ========================================================================
    // Change Streams (Watch)
    // ========================================================================

    /**
     * Set up a change-stream trigger on the collection. Unlike the sync
     * variant, this method does NOT block listening — use
     * `subscribe($conn, "{$collection}_changes", ...)` or iterate
     * `$conn->listen("{$collection}_changes")` directly.
     *
     * If $callback is supplied, the method will ALSO start listening on the
     * channel and invoke the callback for each notification; the call
     * blocks the current fiber until the listener stops.
     */
    public static function docWatch(
        PostgresConnection $conn,
        string $collection,
        ?callable $callback = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        $channel = "{$collection}_changes";
        $funcName = "{$collection}_notify_fn";
        $triggerName = "{$collection}_notify_trg";

        $conn->query("
            CREATE OR REPLACE FUNCTION {$funcName}()
            RETURNS TRIGGER LANGUAGE plpgsql AS \$\$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('{$channel}', json_build_object('op', TG_OP, '_id', OLD._id::text)::text);
                    RETURN OLD;
                ELSE
                    PERFORM pg_notify('{$channel}', json_build_object('op', TG_OP, '_id', NEW._id::text, 'data', NEW.data)::text);
                    RETURN NEW;
                END IF;
            END;
            \$\$
        ");
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $conn->query(
            "CREATE TRIGGER {$triggerName} "
            . "AFTER INSERT OR UPDATE OR DELETE ON {$collection} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );

        if ($callback !== null) {
            $listener = $conn->listen($channel);
            foreach ($listener as $notification) {
                $callback($notification->channel, $notification->payload);
            }
        }
    }

    public static function docUnwatch(PostgresExecutor $conn, string $collection): void
    {
        SyncUtils::validateIdentifier($collection);
        $channel = "{$collection}_changes";
        $funcName = "{$collection}_notify_fn";
        $triggerName = "{$collection}_notify_trg";
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $conn->query("DROP FUNCTION IF EXISTS {$funcName}()");
        // UNLISTEN is transaction-scoped; if this is a bare connection,
        // executing here is harmless. amphp's listen() is cleaned up by
        // calling ->unlisten() on the PostgresListener itself — not here.
        $conn->query("UNLISTEN {$channel}");
    }

    // ========================================================================
    // TTL Indexes
    // ========================================================================

    public static function docCreateTtlIndex(
        PostgresExecutor $conn,
        string $collection,
        int $expireAfterSeconds,
        string $field = 'created_at',
    ): void {
        SyncUtils::validateIdentifier($collection);
        SyncUtils::validateIdentifier($field);
        if ($expireAfterSeconds <= 0) {
            throw new \InvalidArgumentException('expireAfterSeconds must be a positive integer');
        }
        $idxName = "{$collection}_ttl_idx";
        $funcName = "{$collection}_ttl_fn";
        $triggerName = "{$collection}_ttl_trg";
        $conn->query("CREATE INDEX IF NOT EXISTS {$idxName} ON {$collection} ({$field})");
        $conn->query("
            CREATE OR REPLACE FUNCTION {$funcName}()
            RETURNS TRIGGER LANGUAGE plpgsql AS \$\$
            BEGIN
                DELETE FROM {$collection} WHERE {$field} < NOW() - INTERVAL '{$expireAfterSeconds} seconds';
                RETURN NEW;
            END;
            \$\$
        ");
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $conn->query(
            "CREATE TRIGGER {$triggerName} "
            . "BEFORE INSERT ON {$collection} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );
    }

    public static function docRemoveTtlIndex(PostgresExecutor $conn, string $collection): void
    {
        SyncUtils::validateIdentifier($collection);
        $idxName = "{$collection}_ttl_idx";
        $funcName = "{$collection}_ttl_fn";
        $triggerName = "{$collection}_ttl_trg";
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $conn->query("DROP FUNCTION IF EXISTS {$funcName}()");
        $conn->query("DROP INDEX IF EXISTS {$idxName}");
    }

    // ========================================================================
    // Capped Collections
    // ========================================================================

    public static function docCreateCapped(
        PostgresExecutor $conn,
        string $collection,
        int $maxDocuments,
    ): void {
        SyncUtils::validateIdentifier($collection);
        if ($maxDocuments <= 0) {
            throw new \InvalidArgumentException('maxDocuments must be a positive integer');
        }
        self::ensureCollection($conn, $collection);
        $funcName = "{$collection}_cap_fn";
        $triggerName = "{$collection}_cap_trg";
        $conn->query("
            CREATE OR REPLACE FUNCTION {$funcName}()
            RETURNS TRIGGER LANGUAGE plpgsql AS \$\$
            BEGIN
                DELETE FROM {$collection} WHERE _id IN (
                    SELECT _id FROM {$collection}
                    ORDER BY created_at ASC, _id ASC
                    LIMIT GREATEST((SELECT COUNT(*) FROM {$collection}) - {$maxDocuments}, 0)
                );
                RETURN NEW;
            END;
            \$\$
        ");
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $conn->query(
            "CREATE TRIGGER {$triggerName} "
            . "AFTER INSERT ON {$collection} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );
    }

    public static function docRemoveCap(PostgresExecutor $conn, string $collection): void
    {
        SyncUtils::validateIdentifier($collection);
        $funcName = "{$collection}_cap_fn";
        $triggerName = "{$collection}_cap_trg";
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $conn->query("DROP FUNCTION IF EXISTS {$funcName}()");
    }
}
