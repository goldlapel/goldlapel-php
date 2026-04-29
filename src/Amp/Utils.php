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
    // Pub/Sub
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

    // ========================================================================
    // Phase 5 Redis-compat families (counter / zset / hash / queue / geo)
    //
    // The proxy owns DDL — these helpers consume `$patterns` returned from
    // `/api/ddl/<family>/create` and translate `$N → ?` via Ddl::toPdoPlaceholders,
    // then translate back to `$N` for amphp inside `self::exec()`.
    // ========================================================================

    /**
     * Pull a query pattern from the proxy's response and translate `$N → ?`.
     * The downstream `self::exec()` call re-translates `?` back to `$N` for
     * amphp — yes, it's a round-trip, but it keeps a single source of truth
     * for SQL strings (sync stays the canonical reference).
     */
    private static function requireFamilyPattern(?array $patterns, string $key, string $family, string $fn): string
    {
        if ($patterns === null || !isset($patterns['query_patterns'])) {
            throw new \RuntimeException(
                "{$fn} requires DDL patterns from the proxy — call via "
                . "\$gl->{$family}s-><verb>(...) rather than Utils::{$fn} directly."
            );
        }
        $qp = $patterns['query_patterns'];
        if (!isset($qp[$key])) {
            throw new \RuntimeException("DDL API response missing pattern '{$key}' for {$fn}");
        }
        return \GoldLapel\Ddl::toPdoPlaceholders($qp[$key]);
    }

    /**
     * Decode a JSONB column value the wrapper got back. amphp/postgres hands
     * us JSONB columns as strings (just like sync PDO). Decode if it's a
     * string; otherwise pass through. If decoding fails, return the input
     * unchanged.
     */
    private static function decodeJsonb(mixed $value): mixed
    {
        if ($value === null) {
            return null;
        }
        if (!is_string($value)) {
            return $value;
        }
        $decoded = json_decode($value, true);
        return json_last_error() === JSON_ERROR_NONE ? $decoded : $value;
    }

    // ----- Counter family ---------------------------------------------------

    public static function counterIncr(PostgresExecutor $conn, string $name, string $key, int $amount = 1, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'incr', 'counter', 'counterIncr');
        $value = self::fetchColumn($conn, $sql, [$key, $amount]);
        return (int) $value;
    }

    public static function counterDecr(PostgresExecutor $conn, string $name, string $key, int $amount = 1, ?array $patterns = null): int
    {
        return self::counterIncr($conn, $name, $key, -$amount, $patterns);
    }

    public static function counterSet(PostgresExecutor $conn, string $name, string $key, int $value, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'set', 'counter', 'counterSet');
        $stored = self::fetchColumn($conn, $sql, [$key, $value]);
        return (int) $stored;
    }

    public static function counterGet(PostgresExecutor $conn, string $name, string $key, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'get', 'counter', 'counterGet');
        $value = self::fetchColumn($conn, $sql, [$key]);
        return $value !== null ? (int) $value : 0;
    }

    public static function counterDelete(PostgresExecutor $conn, string $name, string $key, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'delete', 'counter', 'counterDelete');
        $result = self::exec($conn, $sql, [$key]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function counterCountKeys(PostgresExecutor $conn, string $name, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'count_keys', 'counter', 'counterCountKeys');
        $value = self::fetchColumn($conn, $sql);
        return $value !== null ? (int) $value : 0;
    }

    // ----- Sorted-set (zset) family -----------------------------------------

    public static function zsetAdd(PostgresExecutor $conn, string $name, string $zsetKey, string $member, float $score, ?array $patterns = null): float
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'zadd', 'zset', 'zsetAdd');
        $value = self::fetchColumn($conn, $sql, [$zsetKey, $member, $score]);
        return (float) $value;
    }

    public static function zsetIncrBy(PostgresExecutor $conn, string $name, string $zsetKey, string $member, float $delta = 1.0, ?array $patterns = null): float
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'zincrby', 'zset', 'zsetIncrBy');
        $value = self::fetchColumn($conn, $sql, [$zsetKey, $member, $delta]);
        return (float) $value;
    }

    public static function zsetScore(PostgresExecutor $conn, string $name, string $zsetKey, string $member, ?array $patterns = null): ?float
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'zscore', 'zset', 'zsetScore');
        $value = self::fetchColumn($conn, $sql, [$zsetKey, $member]);
        return $value !== null ? (float) $value : null;
    }

    public static function zsetRemove(PostgresExecutor $conn, string $name, string $zsetKey, string $member, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'zrem', 'zset', 'zsetRemove');
        $result = self::exec($conn, $sql, [$zsetKey, $member]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function zsetRange(PostgresExecutor $conn, string $name, string $zsetKey, int $start = 0, int $stop = 10, bool $desc = true, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $key = $desc ? 'zrange_desc' : 'zrange_asc';
        $sql = self::requireFamilyPattern($patterns, $key, 'zset', 'zsetRange');
        $limit = max(0, $stop - $start + 1);
        $rows = self::fetchAll($conn, $sql, [$zsetKey, $limit, $start]);
        $out = [];
        foreach ($rows as $r) {
            $out[] = [$r['member'], (float) $r['score']];
        }
        return $out;
    }

    public static function zsetRangeByScore(PostgresExecutor $conn, string $name, string $zsetKey, float $minScore, float $maxScore, int $limit = 100, int $offset = 0, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'zrangebyscore', 'zset', 'zsetRangeByScore');
        $rows = self::fetchAll($conn, $sql, [$zsetKey, $minScore, $maxScore, $limit, $offset]);
        $out = [];
        foreach ($rows as $r) {
            $out[] = [$r['member'], (float) $r['score']];
        }
        return $out;
    }

    public static function zsetRank(PostgresExecutor $conn, string $name, string $zsetKey, string $member, bool $desc = true, ?array $patterns = null): ?int
    {
        SyncUtils::validateIdentifier($name);
        $key = $desc ? 'zrank_desc' : 'zrank_asc';
        $sql = self::requireFamilyPattern($patterns, $key, 'zset', 'zsetRank');
        $value = self::fetchColumn($conn, $sql, [$zsetKey, $member]);
        return $value !== null ? (int) $value : null;
    }

    public static function zsetCard(PostgresExecutor $conn, string $name, string $zsetKey, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'zcard', 'zset', 'zsetCard');
        $value = self::fetchColumn($conn, $sql, [$zsetKey]);
        return $value !== null ? (int) $value : 0;
    }

    // ----- Hash family ------------------------------------------------------

    public static function hashSet(PostgresExecutor $conn, string $name, string $hashKey, string $field, mixed $value, ?array $patterns = null): mixed
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hset', 'hash', 'hashSet');
        $row = self::fetchOne($conn, $sql, [$hashKey, $field, json_encode($value)]);
        if ($row === null) {
            return null;
        }
        return self::decodeJsonb(reset($row));
    }

    public static function hashGet(PostgresExecutor $conn, string $name, string $hashKey, string $field, ?array $patterns = null): mixed
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hget', 'hash', 'hashGet');
        $value = self::fetchColumn($conn, $sql, [$hashKey, $field]);
        return self::decodeJsonb($value);
    }

    public static function hashGetAll(PostgresExecutor $conn, string $name, string $hashKey, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hgetall', 'hash', 'hashGetAll');
        $rows = self::fetchAll($conn, $sql, [$hashKey]);
        $out = [];
        foreach ($rows as $r) {
            $out[(string) $r['field']] = self::decodeJsonb($r['value'] ?? null);
        }
        return $out;
    }

    public static function hashKeys(PostgresExecutor $conn, string $name, string $hashKey, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hkeys', 'hash', 'hashKeys');
        $rows = self::fetchAll($conn, $sql, [$hashKey]);
        return array_map(static fn ($r) => (string) $r['field'], $rows);
    }

    public static function hashValues(PostgresExecutor $conn, string $name, string $hashKey, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hvals', 'hash', 'hashValues');
        $rows = self::fetchAll($conn, $sql, [$hashKey]);
        return array_map(static fn ($r) => self::decodeJsonb($r['value'] ?? null), $rows);
    }

    public static function hashExists(PostgresExecutor $conn, string $name, string $hashKey, string $field, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hexists', 'hash', 'hashExists');
        $value = self::fetchColumn($conn, $sql, [$hashKey, $field]);
        if ($value === null) {
            return false;
        }
        if (is_bool($value)) {
            return $value;
        }
        if (is_string($value)) {
            return $value === 't' || $value === '1' || strcasecmp($value, 'true') === 0;
        }
        return (bool) $value;
    }

    public static function hashDelete(PostgresExecutor $conn, string $name, string $hashKey, string $field, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hdel', 'hash', 'hashDelete');
        $result = self::exec($conn, $sql, [$hashKey, $field]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function hashLen(PostgresExecutor $conn, string $name, string $hashKey, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'hlen', 'hash', 'hashLen');
        $value = self::fetchColumn($conn, $sql, [$hashKey]);
        return $value !== null ? (int) $value : 0;
    }

    // ----- Queue family (at-least-once with visibility timeout) -------------

    public static function queueEnqueue(PostgresExecutor $conn, string $name, mixed $payload, ?array $patterns = null): ?int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'enqueue', 'queue', 'queueEnqueue');
        $row = self::fetchOne($conn, $sql, [json_encode($payload)]);
        if ($row === null) {
            return null;
        }
        return (int) reset($row);
    }

    public static function queueClaim(PostgresExecutor $conn, string $name, int $visibilityTimeoutMs = 30000, ?array $patterns = null): ?array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'claim', 'queue', 'queueClaim');
        $row = self::fetchOne($conn, $sql, [$visibilityTimeoutMs]);
        if ($row === null) {
            return null;
        }
        return [
            'id' => (int) ($row['id'] ?? 0),
            'payload' => self::decodeJsonb($row['payload'] ?? null),
        ];
    }

    public static function queueAck(PostgresExecutor $conn, string $name, int $messageId, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'ack', 'queue', 'queueAck');
        $result = self::exec($conn, $sql, [$messageId]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function queueAbandon(PostgresExecutor $conn, string $name, int $messageId, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'nack', 'queue', 'queueAbandon');
        $row = self::fetchOne($conn, $sql, [$messageId]);
        return $row !== null;
    }

    public static function queueExtend(PostgresExecutor $conn, string $name, int $messageId, int $additionalMs, ?array $patterns = null): mixed
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'extend', 'queue', 'queueExtend');
        // Proxy contract: $1=id, $2=additional_ms (source order). After
        // `$N → ?` bindings appear in source order — bind [id, additional_ms].
        $row = self::fetchOne($conn, $sql, [$messageId, $additionalMs]);
        if ($row === null) {
            return null;
        }
        return reset($row);
    }

    public static function queuePeek(PostgresExecutor $conn, string $name, ?array $patterns = null): ?array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'peek', 'queue', 'queuePeek');
        $row = self::fetchOne($conn, $sql);
        if ($row === null) {
            return null;
        }
        return [
            'id' => (int) ($row['id'] ?? 0),
            'payload' => self::decodeJsonb($row['payload'] ?? null),
            'visible_at' => $row['visible_at'] ?? null,
            'status' => $row['status'] ?? null,
            'created_at' => $row['created_at'] ?? null,
        ];
    }

    public static function queueCountReady(PostgresExecutor $conn, string $name, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'count_ready', 'queue', 'queueCountReady');
        $value = self::fetchColumn($conn, $sql);
        return $value !== null ? (int) $value : 0;
    }

    public static function queueCountClaimed(PostgresExecutor $conn, string $name, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'count_claimed', 'queue', 'queueCountClaimed');
        $value = self::fetchColumn($conn, $sql);
        return $value !== null ? (int) $value : 0;
    }

    // ----- Geo family (PostGIS GEOGRAPHY-native) ----------------------------

    private const GEO_UNIT_FACTORS = [
        'm' => 1.0,
        'km' => 1000.0,
        'mi' => 1609.344,
        'ft' => 0.3048,
    ];

    private static function geoToMeters(float $value, string $unit): float
    {
        if (!isset(self::GEO_UNIT_FACTORS[$unit])) {
            throw new \InvalidArgumentException(
                "Unknown distance unit: '{$unit}' (choose m/km/mi/ft)"
            );
        }
        return $value * self::GEO_UNIT_FACTORS[$unit];
    }

    private static function geoFromMeters(float $meters, string $unit): float
    {
        if (!isset(self::GEO_UNIT_FACTORS[$unit])) {
            throw new \InvalidArgumentException(
                "Unknown distance unit: '{$unit}' (choose m/km/mi/ft)"
            );
        }
        return $meters / self::GEO_UNIT_FACTORS[$unit];
    }

    public static function geoAdd(PostgresExecutor $conn, string $name, string $member, float $lon, float $lat, ?array $patterns = null): ?array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'geoadd', 'geo', 'geoAdd');
        $row = self::fetchOne($conn, $sql, [$member, $lon, $lat]);
        if ($row === null) {
            return null;
        }
        return [(float) ($row['lon'] ?? 0.0), (float) ($row['lat'] ?? 0.0)];
    }

    public static function geoPos(PostgresExecutor $conn, string $name, string $member, ?array $patterns = null): ?array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'geopos', 'geo', 'geoPos');
        $row = self::fetchOne($conn, $sql, [$member]);
        if ($row === null) {
            return null;
        }
        return [(float) ($row['lon'] ?? 0.0), (float) ($row['lat'] ?? 0.0)];
    }

    public static function geoDist(PostgresExecutor $conn, string $name, string $memberA, string $memberB, string $unit = 'm', ?array $patterns = null): ?float
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'geodist', 'geo', 'geoDist');
        $value = self::fetchColumn($conn, $sql, [$memberA, $memberB]);
        if ($value === null) {
            return null;
        }
        return self::geoFromMeters((float) $value, $unit);
    }

    /**
     * Members within `$radius` of (`$lon`, `$lat`).
     *
     * Proxy contract: `$1=lon, $2=lat, $3=radius_m, $4=limit`. The proxy's
     * CTE-anchor pattern means each `$N` appears exactly once in the SQL —
     * after `$N → ?` translation we bind `[lon, lat, radius_m, limit]` in
     * source order.
     */
    public static function geoRadius(PostgresExecutor $conn, string $name, float $lon, float $lat, float $radius, string $unit = 'm', int $limit = 50, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'georadius_with_dist', 'geo', 'geoRadius');
        $radiusM = self::geoToMeters($radius, $unit);
        return self::fetchAll($conn, $sql, [$lon, $lat, $radiusM, $limit]);
    }

    /**
     * Members within `$radius` of `$member`'s location.
     *
     * Proxy contract: `$1`+`$2` both bind the anchor member name (one for the
     * join, one for the self-exclusion); `$3=radius_m, $4=limit`. After
     * `$N → ?` translation, source-text order yields four `?` markers, so we
     * bind `[member, member, radius_m, limit]`.
     */
    public static function geoRadiusByMember(PostgresExecutor $conn, string $name, string $member, float $radius, string $unit = 'm', int $limit = 50, ?array $patterns = null): array
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'geosearch_member', 'geo', 'geoRadiusByMember');
        $radiusM = self::geoToMeters($radius, $unit);
        return self::fetchAll($conn, $sql, [$member, $member, $radiusM, $limit]);
    }

    public static function geoRemove(PostgresExecutor $conn, string $name, string $member, ?array $patterns = null): bool
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'geo_remove', 'geo', 'geoRemove');
        $result = self::exec($conn, $sql, [$member]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function geoCount(PostgresExecutor $conn, string $name, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($name);
        $sql = self::requireFamilyPattern($patterns, 'geo_count', 'geo', 'geoCount');
        $value = self::fetchColumn($conn, $sql);
        return $value !== null ? (int) $value : 0;
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

    public static function streamAdd(PostgresExecutor $conn, string $stream, array $payload, ?array $patterns = null): int
    {
        SyncUtils::validateIdentifier($stream);
        $sql = self::requireFamilyPattern($patterns, 'insert', 'stream', 'streamAdd');
        $withCast = str_replace('VALUES (?)', 'VALUES (?::jsonb)', $sql);
        $id = self::fetchColumn($conn, $withCast, [json_encode($payload)]);
        return (int) $id;
    }

    public static function streamCreateGroup(PostgresExecutor $conn, string $stream, string $group, ?array $patterns = null): void
    {
        SyncUtils::validateIdentifier($stream);
        $sql = self::requireFamilyPattern($patterns, 'create_group', 'stream', 'streamCreateGroup');
        self::exec($conn, $sql, [$group]);
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
        ?array $patterns = null,
    ): array {
        SyncUtils::validateIdentifier($stream);
        $cursorSql = self::requireFamilyPattern($patterns, 'group_get_cursor', 'stream', 'streamRead');
        $readSql = self::requireFamilyPattern($patterns, 'read_since', 'stream', 'streamRead');
        $advanceSql = self::requireFamilyPattern($patterns, 'group_advance_cursor', 'stream', 'streamRead');
        $pendingSql = self::requireFamilyPattern($patterns, 'pending_insert', 'stream', 'streamRead');

        $tx = $conn->beginTransaction();
        try {
            $lastId = self::fetchColumn($tx, $cursorSql, [$group]);
            if ($lastId === null) {
                $tx->commit();
                return [];
            }

            $messages = self::fetchAll($tx, $readSql, [(int) $lastId, $count]);
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
                self::exec($tx, $pendingSql, [$id, $group, $consumer]);
            }
            unset($msg);

            self::exec($tx, $advanceSql, [$maxId, $group]);
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
        ?array $patterns = null,
    ): bool {
        SyncUtils::validateIdentifier($stream);
        $sql = self::requireFamilyPattern($patterns, 'ack', 'stream', 'streamAck');
        $result = self::exec($conn, $sql, [$group, $messageId]);
        return ($result->getRowCount() ?? 0) > 0;
    }

    public static function streamClaim(
        PostgresExecutor $conn,
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMs = 60000,
        ?array $patterns = null,
    ): array {
        SyncUtils::validateIdentifier($stream);
        $claimSql = self::requireFamilyPattern($patterns, 'claim', 'stream', 'streamClaim');
        $readByIdSql = self::requireFamilyPattern($patterns, 'read_by_id', 'stream', 'streamClaim');

        $claimedRows = self::fetchAll($conn, $claimSql, [$consumer, $group, $minIdleMs]);
        $claimed = array_map(fn($r) => (int) $r['message_id'], $claimedRows);
        if (empty($claimed)) {
            return [];
        }
        $messages = [];
        foreach ($claimed as $msgId) {
            $rows = self::fetchAll($conn, $readByIdSql, [$msgId]);
            foreach ($rows as $msg) {
                if (is_string($msg['payload'])) {
                    $msg['payload'] = json_decode($msg['payload'], true);
                }
                $messages[] = $msg;
            }
        }
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

    /**
     * Resolve the canonical doc-store table name from proxy-fetched patterns.
     *
     * Phase 4 schema-to-core: the proxy owns doc-store DDL. The async
     * Documents sub-API issues `/api/ddl/doc_store/create` (idempotent)
     * before dispatching here; by the time these helpers run, the canonical
     * `_goldlapel.doc_<name>` table already exists upstream. Throws if
     * `$patterns` is null — direct util callers must come through
     * `$gl->documents-><verb>` so the sub-API can fetch & cache the DDL.
     */
    private static function docTable(?array $patterns): string
    {
        if ($patterns === null || !isset($patterns['tables']['main'])) {
            throw new \RuntimeException(
                'doc* utils now require DDL patterns from the proxy — call via '
                . '`$gl->documents-><verb>(...)` rather than the Utils function directly.'
            );
        }
        return $patterns['tables']['main'];
    }

    /**
     * Build a deterministic auxiliary index/trigger/function name keyed off
     * a (possibly schema-qualified) table reference. Strips any `schema.`
     * prefix so the name doesn't contain a dot — Postgres rejects those
     * without quoting.
     */
    private static function docAuxName(string $table, string $suffix): string
    {
        $parts = explode('.', $table);
        $bare = end($parts);
        return "{$bare}_{$suffix}";
    }

    public static function docCreateCollection(
        PostgresExecutor $conn,
        string $collection,
        bool $unlogged = false,
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        self::docTable($patterns); // validates patterns; proxy already created the table
    }

    public static function docInsert(
        PostgresExecutor $conn,
        string $collection,
        array $document,
        ?array $patterns = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        return self::fetchOne(
            $conn,
            "INSERT INTO {$table} (data) VALUES (?::jsonb) RETURNING _id, data, created_at",
            [json_encode($document)]
        );
    }

    public static function docInsertMany(
        PostgresExecutor $conn,
        string $collection,
        array $documents,
        ?array $patterns = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        if (empty($documents)) {
            return [];
        }
        $table = self::docTable($patterns);
        $placeholders = implode(', ', array_map(fn() => '(?::jsonb)', $documents));
        $params = array_map(fn($d) => json_encode($d), $documents);
        return self::fetchAll(
            $conn,
            "INSERT INTO {$table} (data) VALUES {$placeholders} RETURNING _id, data, created_at",
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
        ?array $patterns = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$table}";
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
        ?array $patterns = null,
    ): \Amp\Pipeline\ConcurrentIterator {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$table}";
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
        ?array $patterns = null,
    ): ?array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$table}";
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
        ?array $patterns = null,
    ): int {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        [$updateExpr, $updateParams] = SyncUtils::buildUpdate($update);
        $where = $clause !== '' ? $clause : 'TRUE';
        $allParams = [...$updateParams, ...$filterParams];
        $result = self::exec(
            $conn,
            "UPDATE {$table} SET data = {$updateExpr} WHERE {$where}",
            $allParams
        );
        return $result->getRowCount() ?? 0;
    }

    public static function docUpdateOne(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        array $update,
        ?array $patterns = null,
    ): int {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        [$updateExpr, $updateParams] = SyncUtils::buildUpdate($update);
        $where = $clause !== '' ? $clause : 'TRUE';
        $allParams = [...$filterParams, ...$updateParams];
        $result = self::exec($conn,
            "WITH target AS ("
            . "SELECT _id FROM {$table} WHERE {$where} LIMIT 1"
            . ") UPDATE {$table} SET data = {$updateExpr} "
            . "FROM target WHERE {$table}._id = target._id",
            $allParams
        );
        return $result->getRowCount() ?? 0;
    }

    public static function docDelete(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        ?array $patterns = null,
    ): int {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $result = self::exec($conn, "DELETE FROM {$table} WHERE {$where}", $params);
        return $result->getRowCount() ?? 0;
    }

    public static function docDeleteOne(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        ?array $patterns = null,
    ): int {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $result = self::exec($conn,
            "WITH target AS ("
            . "SELECT _id FROM {$table} WHERE {$where} LIMIT 1"
            . ") DELETE FROM {$table} USING target WHERE {$table}._id = target._id",
            $params
        );
        return $result->getRowCount() ?? 0;
    }

    public static function docCount(
        PostgresExecutor $conn,
        string $collection,
        ?array $filter = null,
        ?array $patterns = null,
    ): int {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $params] = SyncUtils::buildFilter($filter);
        $sql = "SELECT COUNT(*) FROM {$table}";
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
        ?array $patterns = null,
    ): ?array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        [$updateExpr, $updateParams] = SyncUtils::buildUpdate($update);
        $cteWhere = $clause !== '' ? " WHERE {$clause}" : '';
        $sql = "WITH target AS ("
            . "SELECT _id FROM {$table}{$cteWhere} LIMIT 1"
            . ") UPDATE {$table} SET data = {$updateExpr} FROM target "
            . "WHERE {$table}._id = target._id "
            . "RETURNING {$table}._id, {$table}.data, {$table}.created_at";
        $allParams = [...$filterParams, ...$updateParams];
        return self::fetchOne($conn, $sql, $allParams);
    }

    public static function docFindOneAndDelete(
        PostgresExecutor $conn,
        string $collection,
        array $filter,
        ?array $patterns = null,
    ): ?array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        [$clause, $filterParams] = SyncUtils::buildFilter($filter);
        $cteWhere = $clause !== '' ? " WHERE {$clause}" : '';
        $sql = "WITH target AS ("
            . "SELECT _id FROM {$table}{$cteWhere} LIMIT 1"
            . ") DELETE FROM {$table} USING target "
            . "WHERE {$table}._id = target._id "
            . "RETURNING {$table}._id, {$table}.data, {$table}.created_at";
        return self::fetchOne($conn, $sql, $filterParams);
    }

    public static function docDistinct(
        PostgresExecutor $conn,
        string $collection,
        string $field,
        ?array $filter = null,
        ?array $patterns = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        $fieldExpr = SyncUtils::fieldPath($field);
        $sql = "SELECT DISTINCT {$fieldExpr} FROM {$table}";
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
        ?array $patterns = null,
        ?array $lookupTables = null,
    ): array {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);

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
            // Resolve `from` to its canonical proxy table when the documents
            // sub-API supplied a lookup map (Phase 4 schema-to-core). Direct
            // util callers without a map keep the legacy public-schema name.
            $fromTable = ($lookupTables !== null && isset($lookupTables[$from]))
                ? $lookupTables[$from]
                : $from;
            $lookupSqls[] = "COALESCE((SELECT json_agg(_lk.data) FROM {$fromTable} _lk WHERE _lk.data->>'{$foreignField}' = {$table}.data->>'{$localField}'), '[]'::json) AS {$asField}";
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
            $sql = "SELECT " . implode(', ', $selectFields) . " FROM {$table}";
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
            $sql = "SELECT " . implode(', ', $allParts) . " FROM {$table}";
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
            $sql = "SELECT " . implode(', ', $baseParts) . " FROM {$table}";
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
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        if ($keys === null || count($keys) === 0) {
            $idx = self::docAuxName($table, 'data_gin');
            $conn->query(
                "CREATE INDEX IF NOT EXISTS {$idx} ON {$table} USING GIN (data)"
            );
            return;
        }
        foreach ($keys as $key => $dir) {
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                throw new \InvalidArgumentException("Invalid index key: {$key}");
            }
            $order = $dir === -1 ? 'DESC' : 'ASC';
            $safeName = str_replace('.', '_', $key);
            $idx = self::docAuxName($table, "{$safeName}_idx");
            $conn->query(
                "CREATE INDEX IF NOT EXISTS {$idx} "
                . "ON {$table} ((data->>'{$key}') {$order})"
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
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        // Channel/function/trigger names key off the user's collection name
        // (validated identifier — safe to interpolate). Triggers fire on the
        // canonical proxy table.
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
        // CREATE OR REPLACE TRIGGER (Postgres 14+) is atomic — no window
        // where the trigger is missing between DROP and CREATE, and a
        // redefinition cleanly replaces the old body. GL targets PG14+
        // across the product, so this is safe and matches the Go wrapper.
        $conn->query(
            "CREATE OR REPLACE TRIGGER {$triggerName} "
            . "AFTER INSERT OR UPDATE OR DELETE ON {$table} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );

        if ($callback !== null) {
            $listener = $conn->listen($channel);
            foreach ($listener as $notification) {
                $callback($notification->channel, $notification->payload);
            }
        }
    }

    public static function docUnwatch(
        PostgresExecutor $conn,
        string $collection,
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        $channel = "{$collection}_changes";
        $funcName = "{$collection}_notify_fn";
        $triggerName = "{$collection}_notify_trg";
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$table}");
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
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        SyncUtils::validateIdentifier($field);
        if ($expireAfterSeconds <= 0) {
            throw new \InvalidArgumentException('expireAfterSeconds must be a positive integer');
        }
        $table = self::docTable($patterns);
        $idxName = self::docAuxName($table, 'ttl_idx');
        $funcName = "{$collection}_ttl_fn";
        $triggerName = "{$collection}_ttl_trg";
        $conn->query("CREATE INDEX IF NOT EXISTS {$idxName} ON {$table} ({$field})");
        $conn->query("
            CREATE OR REPLACE FUNCTION {$funcName}()
            RETURNS TRIGGER LANGUAGE plpgsql AS \$\$
            BEGIN
                DELETE FROM {$table} WHERE {$field} < NOW() - INTERVAL '{$expireAfterSeconds} seconds';
                RETURN NEW;
            END;
            \$\$
        ");
        // CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and redefinable.
        // See docWatch for rationale.
        $conn->query(
            "CREATE OR REPLACE TRIGGER {$triggerName} "
            . "BEFORE INSERT ON {$table} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );
    }

    public static function docRemoveTtlIndex(
        PostgresExecutor $conn,
        string $collection,
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        $idxName = self::docAuxName($table, 'ttl_idx');
        $funcName = "{$collection}_ttl_fn";
        $triggerName = "{$collection}_ttl_trg";
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$table}");
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
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        if ($maxDocuments <= 0) {
            throw new \InvalidArgumentException('maxDocuments must be a positive integer');
        }
        // The underlying doc-store table is already materialized by the
        // proxy (Documents::patterns issues create on first call). This
        // call only adds the cap trigger + function.
        $table = self::docTable($patterns);
        $funcName = "{$collection}_cap_fn";
        $triggerName = "{$collection}_cap_trg";
        $conn->query("
            CREATE OR REPLACE FUNCTION {$funcName}()
            RETURNS TRIGGER LANGUAGE plpgsql AS \$\$
            BEGIN
                DELETE FROM {$table} WHERE _id IN (
                    SELECT _id FROM {$table}
                    ORDER BY created_at ASC, _id ASC
                    LIMIT GREATEST((SELECT COUNT(*) FROM {$table}) - {$maxDocuments}, 0)
                );
                RETURN NEW;
            END;
            \$\$
        ");
        // CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and redefinable.
        // See docWatch for rationale.
        $conn->query(
            "CREATE OR REPLACE TRIGGER {$triggerName} "
            . "AFTER INSERT ON {$table} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );
    }

    public static function docRemoveCap(
        PostgresExecutor $conn,
        string $collection,
        ?array $patterns = null,
    ): void {
        SyncUtils::validateIdentifier($collection);
        $table = self::docTable($patterns);
        $funcName = "{$collection}_cap_fn";
        $triggerName = "{$collection}_cap_trg";
        $conn->query("DROP TRIGGER IF EXISTS {$triggerName} ON {$table}");
        $conn->query("DROP FUNCTION IF EXISTS {$funcName}()");
    }
}
