<?php

namespace GoldLapel;

class Utils
{
    private static function validateIdentifier(string $name): string
    {
        if ($name === '' || !preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $name)) {
            throw new \InvalidArgumentException("Invalid identifier: {$name}");
        }
        return $name;
    }

    public static function search(
        \PDO $pdo,
        string $table,
        string|array $column,
        string $query,
        int $limit = 50,
        string $lang = 'english',
        bool $highlight = false,
    ): array {
        self::validateIdentifier($table);
        $columns = is_array($column) ? $column : [$column];
        foreach ($columns as $c) {
            self::validateIdentifier($c);
        }
        $tsvector = implode(" || ' ' || ", array_map(fn($c) => "coalesce({$c}, '')", $columns));
        $tsv = "to_tsvector(?, {$tsvector})";
        $tsq = "plainto_tsquery(?, ?)";
        if ($highlight) {
            $fields = "*, ts_rank({$tsv}, {$tsq}) AS _score, ts_headline(?, {$tsvector}, {$tsq}, 'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight";
            $sql = "SELECT {$fields} FROM {$table} WHERE {$tsv} @@ {$tsq} ORDER BY _score DESC LIMIT ?";
            $stmt = $pdo->prepare($sql);
            // ?-params: ts_rank(tsv[lang], tsq[lang,query]) + ts_headline(lang, tsq[lang,query]) + WHERE tsv[lang] @@ tsq[lang,query] + LIMIT
            $stmt->execute([$lang, $lang, $query, $lang, $lang, $query, $lang, $lang, $query, $limit]);
        } else {
            $fields = "*, ts_rank({$tsv}, {$tsq}) AS _score";
            $sql = "SELECT {$fields} FROM {$table} WHERE {$tsv} @@ {$tsq} ORDER BY _score DESC LIMIT ?";
            $stmt = $pdo->prepare($sql);
            // ?-params: ts_rank(tsv[lang], tsq[lang,query]) + WHERE tsv[lang] @@ tsq[lang,query] + LIMIT
            $stmt->execute([$lang, $lang, $query, $lang, $lang, $query, $limit]);
        }
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function searchFuzzy(
        \PDO $pdo,
        string $table,
        string $column,
        string $query,
        int $limit = 50,
        float $threshold = 0.3,
    ): array {
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        $stmt = $pdo->prepare("
            SELECT *, similarity({$column}, ?) AS _score
            FROM {$table}
            WHERE similarity({$column}, ?) > ?
            ORDER BY _score DESC
            LIMIT ?
        ");
        $stmt->execute([$query, $query, $threshold, $limit]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function searchPhonetic(
        \PDO $pdo,
        string $table,
        string $column,
        string $query,
        int $limit = 50,
    ): array {
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch");
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        $stmt = $pdo->prepare("
            SELECT *, similarity({$column}, ?) AS _score
            FROM {$table}
            WHERE soundex({$column}) = soundex(?)
            ORDER BY _score DESC, {$column}
            LIMIT ?
        ");
        $stmt->execute([$query, $query, $limit]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function similar(
        \PDO $pdo,
        string $table,
        string $column,
        array $vector,
        int $limit = 10,
    ): array {
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS vector");
        $vectorLiteral = '[' . implode(',', $vector) . ']';
        $stmt = $pdo->prepare("
            SELECT *, ({$column} <=> ?::vector) AS _score
            FROM {$table}
            ORDER BY _score
            LIMIT ?
        ");
        $stmt->execute([$vectorLiteral, $limit]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function suggest(
        \PDO $pdo,
        string $table,
        string $column,
        string $prefix,
        int $limit = 10,
    ): array {
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        $stmt = $pdo->prepare("
            SELECT *, similarity({$column}, ?) AS _score
            FROM {$table}
            WHERE {$column} ILIKE ?
            ORDER BY _score DESC, {$column}
            LIMIT ?
        ");
        $stmt->execute([$prefix, $prefix . '%', $limit]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function publish(\PDO $pdo, string $channel, string $message): void
    {
        $stmt = $pdo->prepare("SELECT pg_notify(?, ?)");
        $stmt->execute([$channel, $message]);
    }

    public static function subscribe(\PDO $pdo, string $channel, callable $callback): void
    {
        $pdo->exec("LISTEN " . $channel);
        while (true) {
            $notify = $pdo->pgsqlGetNotify(\PDO::FETCH_ASSOC, 5000);
            if ($notify) {
                $callback($notify['message'], $notify['payload']);
            }
        }
    }

    public static function enqueue(\PDO $pdo, string $queueTable, array $payload): void
    {
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$queueTable} (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ");
        $stmt = $pdo->prepare("INSERT INTO {$queueTable} (payload) VALUES (?)");
        $stmt->execute([json_encode($payload)]);
    }

    public static function dequeue(\PDO $pdo, string $queueTable): ?array
    {
        $stmt = $pdo->query("
            DELETE FROM {$queueTable}
            WHERE id = (
                SELECT id FROM {$queueTable}
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING payload
        ");
        $row = $stmt->fetch(\PDO::FETCH_NUM);
        if ($row === false) {
            return null;
        }
        $value = $row[0];
        return is_array($value) ? $value : json_decode($value, true);
    }

    public static function incr(\PDO $pdo, string $table, string $key, int $amount = 1): int
    {
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$table} (
                key TEXT PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 0
            )
        ");
        $stmt = $pdo->prepare("
            INSERT INTO {$table} (key, value) VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = {$table}.value + ?
            RETURNING value
        ");
        $stmt->execute([$key, $amount, $amount]);
        return (int) $stmt->fetchColumn();
    }

    public static function getCounter(\PDO $pdo, string $table, string $key): int
    {
        $stmt = $pdo->prepare("SELECT value FROM {$table} WHERE key = ?");
        $stmt->execute([$key]);
        $value = $stmt->fetchColumn();
        return $value !== false ? (int) $value : 0;
    }

    public static function zadd(\PDO $pdo, string $table, string $member, float $score): void
    {
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$table} (
                member TEXT PRIMARY KEY,
                score DOUBLE PRECISION NOT NULL
            )
        ");
        $stmt = $pdo->prepare("
            INSERT INTO {$table} (member, score) VALUES (?, ?)
            ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score
        ");
        $stmt->execute([$member, $score]);
    }

    public static function zincrby(\PDO $pdo, string $table, string $member, float $amount = 1): float
    {
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$table} (
                member TEXT PRIMARY KEY,
                score DOUBLE PRECISION NOT NULL
            )
        ");
        $stmt = $pdo->prepare("
            INSERT INTO {$table} (member, score) VALUES (?, ?)
            ON CONFLICT (member) DO UPDATE SET score = {$table}.score + ?
            RETURNING score
        ");
        $stmt->execute([$member, $amount, $amount]);
        return (float) $stmt->fetchColumn();
    }

    public static function zrange(\PDO $pdo, string $table, int $start = 0, int $stop = 10, bool $desc = true): array
    {
        $order = $desc ? 'DESC' : 'ASC';
        $limit = $stop - $start;
        $stmt = $pdo->prepare("
            SELECT member, score FROM {$table}
            ORDER BY score {$order}
            LIMIT ? OFFSET ?
        ");
        $stmt->execute([$limit, $start]);
        return $stmt->fetchAll(\PDO::FETCH_NUM);
    }

    public static function zrank(\PDO $pdo, string $table, string $member, bool $desc = true): ?int
    {
        $order = $desc ? 'DESC' : 'ASC';
        $stmt = $pdo->prepare("
            SELECT rank FROM (
                SELECT member, ROW_NUMBER() OVER (ORDER BY score {$order}) - 1 AS rank
                FROM {$table}
            ) ranked
            WHERE member = ?
        ");
        $stmt->execute([$member]);
        $value = $stmt->fetchColumn();
        return $value !== false ? (int) $value : null;
    }

    public static function zscore(\PDO $pdo, string $table, string $member): ?float
    {
        $stmt = $pdo->prepare("SELECT score FROM {$table} WHERE member = ?");
        $stmt->execute([$member]);
        $value = $stmt->fetchColumn();
        return $value !== false ? (float) $value : null;
    }

    public static function zrem(\PDO $pdo, string $table, string $member): bool
    {
        $stmt = $pdo->prepare("DELETE FROM {$table} WHERE member = ?");
        $stmt->execute([$member]);
        return $stmt->rowCount() > 0;
    }

    public static function geoadd(
        \PDO $pdo,
        string $table,
        string $nameColumn,
        string $geomColumn,
        string $name,
        float $lon,
        float $lat,
    ): void {
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS postgis");
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$table} (
                id BIGSERIAL PRIMARY KEY,
                {$nameColumn} TEXT NOT NULL,
                {$geomColumn} GEOMETRY(Point, 4326) NOT NULL
            )
        ");
        $stmt = $pdo->prepare("
            INSERT INTO {$table} ({$nameColumn}, {$geomColumn})
            VALUES (?, ST_SetSRID(ST_MakePoint(?, ?), 4326))
        ");
        $stmt->execute([$name, $lon, $lat]);
    }

    public static function georadius(
        \PDO $pdo,
        string $table,
        string $geomColumn,
        float $lon,
        float $lat,
        float $radiusMeters,
        int $limit = 50,
    ): array {
        $stmt = $pdo->prepare("
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
        ");
        $stmt->execute([$lon, $lat, $lon, $lat, $radiusMeters, $limit]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function geodist(
        \PDO $pdo,
        string $table,
        string $geomColumn,
        string $nameColumn,
        string $nameA,
        string $nameB,
    ): ?float {
        $stmt = $pdo->prepare("
            SELECT ST_Distance(a.{$geomColumn}::geography, b.{$geomColumn}::geography)
            FROM {$table} a, {$table} b
            WHERE a.{$nameColumn} = ? AND b.{$nameColumn} = ?
        ");
        $stmt->execute([$nameA, $nameB]);
        $value = $stmt->fetchColumn();
        return $value !== false ? (float) $value : null;
    }

    public static function hset(\PDO $pdo, string $table, string $key, string $field, mixed $value): void
    {
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$table} (
                key TEXT PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'::jsonb
            )
        ");
        $json = json_encode($value);
        $stmt = $pdo->prepare("
            INSERT INTO {$table} (key, data) VALUES (?, jsonb_build_object(?, ?::jsonb))
            ON CONFLICT (key) DO UPDATE SET data = {$table}.data || jsonb_build_object(?, ?::jsonb)
        ");
        $stmt->execute([$key, $field, $json, $field, $json]);
    }

    public static function hget(\PDO $pdo, string $table, string $key, string $field): mixed
    {
        $stmt = $pdo->prepare("SELECT data->>? FROM {$table} WHERE key = ?");
        $stmt->execute([$field, $key]);
        $value = $stmt->fetchColumn();
        if ($value === false || $value === null) {
            return null;
        }
        $decoded = json_decode($value, true);
        return json_last_error() === JSON_ERROR_NONE ? $decoded : $value;
    }

    public static function hgetall(\PDO $pdo, string $table, string $key): array
    {
        $stmt = $pdo->prepare("SELECT data FROM {$table} WHERE key = ?");
        $stmt->execute([$key]);
        $value = $stmt->fetchColumn();
        if ($value === false || $value === null) {
            return [];
        }
        return json_decode($value, true) ?? [];
    }

    public static function hdel(\PDO $pdo, string $table, string $key, string $field): bool
    {
        $stmt = $pdo->prepare("SELECT data ? ? AS existed FROM {$table} WHERE key = ?");
        $stmt->execute([$field, $key]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        if ($row === false || !$row['existed']) {
            return false;
        }
        $stmt = $pdo->prepare("UPDATE {$table} SET data = data - ? WHERE key = ?");
        $stmt->execute([$field, $key]);
        return true;
    }

    public static function countDistinct(\PDO $pdo, string $table, string $column): int
    {
        $stmt = $pdo->query("SELECT COUNT(DISTINCT {$column}) FROM {$table}");
        return (int) $stmt->fetchColumn();
    }

    public static function streamAdd(\PDO $pdo, string $stream, array $payload): int
    {
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$stream} (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ");
        $stmt = $pdo->prepare("INSERT INTO {$stream} (payload) VALUES (?) RETURNING id");
        $stmt->execute([json_encode($payload)]);
        return (int) $stmt->fetchColumn();
    }

    public static function streamCreateGroup(\PDO $pdo, string $stream, string $group): void
    {
        $groupsTable = $stream . '_groups';
        $pendingTable = $stream . '_pending';
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$groupsTable} (
                group_name TEXT PRIMARY KEY,
                last_id BIGINT NOT NULL DEFAULT 0
            )
        ");
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$pendingTable} (
                message_id BIGINT NOT NULL,
                group_name TEXT NOT NULL,
                consumer TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (group_name, message_id)
            )
        ");
        $stmt = $pdo->prepare("
            INSERT INTO {$groupsTable} (group_name, last_id) VALUES (?, 0)
            ON CONFLICT (group_name) DO NOTHING
        ");
        $stmt->execute([$group]);
    }

    public static function streamRead(\PDO $pdo, string $stream, string $group, string $consumer, int $count = 1): array
    {
        $groupsTable = $stream . '_groups';
        $pendingTable = $stream . '_pending';

        $pdo->beginTransaction();
        try {
            $stmt = $pdo->prepare("SELECT last_id FROM {$groupsTable} WHERE group_name = ? FOR UPDATE");
            $stmt->execute([$group]);
            $lastId = $stmt->fetchColumn();
            if ($lastId === false) {
                $pdo->commit();
                return [];
            }

            $stmt = $pdo->prepare("
                SELECT id, payload, created_at FROM {$stream}
                WHERE id > ?
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            ");
            $stmt->execute([$lastId, $count]);
            $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            if (empty($messages)) {
                $pdo->commit();
                return [];
            }

            $maxId = 0;
            $insert = $pdo->prepare("
                INSERT INTO {$pendingTable} (message_id, group_name, consumer)
                VALUES (?, ?, ?)
                ON CONFLICT (group_name, message_id) DO NOTHING
            ");
            foreach ($messages as &$msg) {
                $msg['payload'] = json_decode($msg['payload'], true);
                $id = (int) $msg['id'];
                if ($id > $maxId) {
                    $maxId = $id;
                }
                $insert->execute([$id, $group, $consumer]);
            }
            unset($msg);

            $stmt = $pdo->prepare("UPDATE {$groupsTable} SET last_id = ? WHERE group_name = ?");
            $stmt->execute([$maxId, $group]);

            $pdo->commit();
            return $messages;
        } catch (\Throwable $e) {
            $pdo->rollBack();
            throw $e;
        }
    }

    public static function streamAck(\PDO $pdo, string $stream, string $group, int $messageId): bool
    {
        $pendingTable = $stream . '_pending';
        $stmt = $pdo->prepare("DELETE FROM {$pendingTable} WHERE group_name = ? AND message_id = ?");
        $stmt->execute([$group, $messageId]);
        return $stmt->rowCount() > 0;
    }

    public static function streamClaim(\PDO $pdo, string $stream, string $group, string $consumer, int $minIdleMs = 60000): array
    {
        $pendingTable = $stream . '_pending';
        $stmt = $pdo->prepare("
            UPDATE {$pendingTable}
            SET consumer = ?, assigned_at = NOW()
            WHERE group_name = ?
            AND assigned_at < NOW() - (? || ' milliseconds')::interval
            RETURNING message_id
        ");
        $stmt->execute([$consumer, $group, $minIdleMs]);
        $claimed = $stmt->fetchAll(\PDO::FETCH_COLUMN);

        if (empty($claimed)) {
            return [];
        }

        $placeholders = implode(', ', array_fill(0, count($claimed), '?'));
        $stmt = $pdo->prepare("
            SELECT id, payload, created_at FROM {$stream}
            WHERE id IN ({$placeholders})
            ORDER BY id
        ");
        $stmt->execute($claimed);
        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($messages as &$msg) {
            $msg['payload'] = json_decode($msg['payload'], true);
        }
        unset($msg);

        return $messages;
    }

    public static function facets(
        \PDO $pdo,
        string $table,
        string $column,
        int $limit = 50,
        ?string $query = null,
        string|array|null $queryColumn = null,
        string $lang = 'english',
    ): array {
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        if ($query !== null && $queryColumn !== null) {
            $columns = is_array($queryColumn) ? $queryColumn : [$queryColumn];
            foreach ($columns as $c) {
                self::validateIdentifier($c);
            }
            $tsvector = implode(" || ' ' || ", array_map(fn($c) => "coalesce({$c}, '')", $columns));
            $sql = "SELECT {$column} AS value, COUNT(*) AS count FROM {$table} WHERE to_tsvector(?, {$tsvector}) @@ plainto_tsquery(?, ?) GROUP BY {$column} ORDER BY count DESC, {$column} LIMIT ?";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$lang, $lang, $query, $limit]);
        } else {
            $sql = "SELECT {$column} AS value, COUNT(*) AS count FROM {$table} GROUP BY {$column} ORDER BY count DESC, {$column} LIMIT ?";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$limit]);
        }
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function aggregate(
        \PDO $pdo,
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
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        $funcUpper = strtoupper($func);
        $expr = $funcUpper === 'COUNT' ? 'COUNT(*)' : "{$funcUpper}({$column})";
        if ($groupBy !== null) {
            self::validateIdentifier($groupBy);
            $sql = "SELECT {$groupBy}, {$expr} AS value FROM {$table} GROUP BY {$groupBy} ORDER BY value DESC LIMIT ?";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$limit]);
        } else {
            $sql = "SELECT {$expr} AS value FROM {$table}";
            $stmt = $pdo->prepare($sql);
            $stmt->execute();
        }
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function createSearchConfig(\PDO $pdo, string $name, string $copyFrom = 'english'): void
    {
        self::validateIdentifier($name);
        self::validateIdentifier($copyFrom);
        $stmt = $pdo->prepare("SELECT 1 FROM pg_ts_config WHERE cfgname = ?");
        $stmt->execute([$name]);
        if ($stmt->fetchColumn() !== false) {
            return;
        }
        $pdo->exec("CREATE TEXT SEARCH CONFIGURATION {$name} (COPY = {$copyFrom})");
    }

    public static function percolateAdd(
        \PDO $pdo,
        string $name,
        string $queryId,
        string $query,
        string $lang = 'english',
        ?array $metadata = null,
    ): void {
        self::validateIdentifier($name);
        $pdo->exec("
            CREATE TABLE IF NOT EXISTS {$name} (
                query_id TEXT PRIMARY KEY,
                query_text TEXT NOT NULL,
                tsquery TSQUERY NOT NULL,
                lang TEXT NOT NULL DEFAULT 'english',
                metadata JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ");
        $pdo->exec("CREATE INDEX IF NOT EXISTS {$name}_tsq_idx ON {$name} USING GIN (tsquery)");
        $stmt = $pdo->prepare("
            INSERT INTO {$name} (query_id, query_text, tsquery, lang, metadata)
            VALUES (?, ?, plainto_tsquery(?, ?), ?, ?)
            ON CONFLICT (query_id) DO UPDATE SET
                query_text = EXCLUDED.query_text,
                tsquery = EXCLUDED.tsquery,
                lang = EXCLUDED.lang,
                metadata = EXCLUDED.metadata
        ");
        $stmt->execute([
            $queryId,
            $query,
            $lang,
            $query,
            $lang,
            $metadata !== null ? json_encode($metadata) : null,
        ]);
    }

    public static function percolate(
        \PDO $pdo,
        string $name,
        string $text,
        int $limit = 50,
        string $lang = 'english',
    ): array {
        self::validateIdentifier($name);
        $stmt = $pdo->prepare("
            SELECT query_id, query_text, metadata,
                ts_rank(to_tsvector(?, ?), tsquery) AS _score
            FROM {$name}
            WHERE to_tsvector(?, ?) @@ tsquery
            ORDER BY _score DESC
            LIMIT ?
        ");
        $stmt->execute([$lang, $text, $lang, $text, $limit]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function percolateDelete(\PDO $pdo, string $name, string $queryId): bool
    {
        self::validateIdentifier($name);
        $stmt = $pdo->prepare("DELETE FROM {$name} WHERE query_id = ? RETURNING query_id");
        $stmt->execute([$queryId]);
        return $stmt->rowCount() > 0;
    }

    public static function analyze(\PDO $pdo, string $text, string $lang = 'english'): array
    {
        $stmt = $pdo->prepare("SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug(?, ?)");
        $stmt->execute([$lang, $text]);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function explainScore(
        \PDO $pdo,
        string $table,
        string $column,
        string $query,
        string $idColumn,
        mixed $idValue,
        string $lang = 'english',
    ): ?array {
        self::validateIdentifier($table);
        self::validateIdentifier($column);
        self::validateIdentifier($idColumn);
        $sql = "SELECT {$column} AS document_text, to_tsvector(?, {$column})::text AS document_tokens, "
            . "plainto_tsquery(?, ?)::text AS query_tokens, "
            . "to_tsvector(?, {$column}) @@ plainto_tsquery(?, ?) AS matches, "
            . "ts_rank(to_tsvector(?, {$column}), plainto_tsquery(?, ?)) AS score, "
            . "ts_headline(?, {$column}, plainto_tsquery(?, ?), 'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline "
            . "FROM {$table} WHERE {$idColumn} = ?";
        $stmt = $pdo->prepare($sql);
        $stmt->execute([$lang, $lang, $query, $lang, $lang, $query, $lang, $lang, $query, $lang, $lang, $query, $idValue]);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        return $row !== false ? $row : null;
    }

    public static function script(\PDO $pdo, string $luaCode, mixed ...$args): ?string
    {
        $pdo->exec("CREATE EXTENSION IF NOT EXISTS pllua");
        $funcName = "_gl_lua_" . bin2hex(random_bytes(4));
        $params = implode(", ", array_map(fn($i) => "p" . ($i + 1) . " text", range(0, count($args) - 1)));
        $pdo->exec("CREATE OR REPLACE FUNCTION pg_temp.{$funcName}({$params}) RETURNS text LANGUAGE pllua AS \$pllua\$ {$luaCode} \$pllua\$");
        if (empty($args)) {
            $stmt = $pdo->query("SELECT pg_temp.{$funcName}()");
        } else {
            $placeholders = implode(", ", array_map(fn($i) => "?", range(0, count($args) - 1)));
            $stmt = $pdo->prepare("SELECT pg_temp.{$funcName}({$placeholders})");
            $stmt->execute(array_map('strval', $args));
        }
        $row = $stmt->fetch(\PDO::FETCH_NUM);
        return $row ? $row[0] : null;
    }

    // ========================================================================
    // Document Store
    // ========================================================================

    private static $COMPARISON_OPS = [
        '$gt' => '>', '$gte' => '>=', '$lt' => '<', '$lte' => '<=',
        '$eq' => '=', '$ne' => '!=',
    ];

    private static function fieldPath(string $key): string
    {
        $parts = explode('.', $key);
        foreach ($parts as $part) {
            if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $part)) {
                throw new \InvalidArgumentException("Invalid filter key: {$key}");
            }
        }
        if (count($parts) === 1) {
            return "data->>'{$parts[0]}'";
        }
        $path = 'data';
        for ($i = 0; $i < count($parts) - 1; $i++) {
            $path .= "->'{$parts[$i]}'";
        }
        $path .= "->>'{$parts[count($parts) - 1]}'";
        return $path;
    }

    private static function resolveFieldRef(string $ref, array $unwindMap = []): string
    {
        $field = str_starts_with($ref, '$') ? substr($ref, 1) : $ref;
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $field)) {
            throw new \InvalidArgumentException("Invalid field reference: \${$field}");
        }
        if (isset($unwindMap[$field])) {
            return $unwindMap[$field];
        }
        return self::fieldPath($field);
    }

    public static function expandDotKeys(array $d): array
    {
        $result = [];
        foreach ($d as $key => $value) {
            $parts = explode('.', $key);
            $current = &$result;
            foreach (array_slice($parts, 0, -1) as $part) {
                if (!isset($current[$part])) {
                    $current[$part] = [];
                }
                $current = &$current[$part];
            }
            $current[$parts[count($parts) - 1]] = $value;
            unset($current);
        }
        return $result;
    }

    private static function buildFilter(?array $filter): array
    {
        if ($filter === null || count($filter) === 0) {
            return ['', []];
        }

        $containment = [];
        $opClauses = [];
        $params = [];

        foreach ($filter as $key => $value) {
            if (is_array($value) && !array_is_list($value) && self::hasOperators($value)) {
                $fp = self::fieldPath($key);

                foreach ($value as $op => $operand) {
                    if (isset(self::$COMPARISON_OPS[$op])) {
                        $sqlOp = self::$COMPARISON_OPS[$op];
                        if (is_int($operand) || is_float($operand)) {
                            $opClauses[] = "({$fp})::numeric {$sqlOp} ?";
                            $params[] = $operand;
                        } else {
                            $opClauses[] = "{$fp} {$sqlOp} ?";
                            $params[] = (string) $operand;
                        }
                    } elseif ($op === '$in') {
                        if (!is_array($operand) || count($operand) === 0) {
                            throw new \InvalidArgumentException('$in requires a non-empty array');
                        }
                        $placeholders = implode(', ', array_fill(0, count($operand), '?'));
                        $opClauses[] = "{$fp} IN ({$placeholders})";
                        foreach ($operand as $item) {
                            $params[] = (string) $item;
                        }
                    } elseif ($op === '$nin') {
                        if (!is_array($operand) || count($operand) === 0) {
                            throw new \InvalidArgumentException('$nin requires a non-empty array');
                        }
                        $placeholders = implode(', ', array_fill(0, count($operand), '?'));
                        $opClauses[] = "{$fp} NOT IN ({$placeholders})";
                        foreach ($operand as $item) {
                            $params[] = (string) $item;
                        }
                    } elseif ($op === '$exists') {
                        $parts = explode('.', $key);
                        $topKey = $parts[0];
                        if ($operand) {
                            $opClauses[] = "data ? ?";
                        } else {
                            $opClauses[] = "NOT (data ? ?)";
                        }
                        $params[] = $topKey;
                    } elseif ($op === '$regex') {
                        $opClauses[] = "{$fp} ~ ?";
                        $params[] = $operand;
                    } else {
                        throw new \InvalidArgumentException("Unknown filter operator: {$op}");
                    }
                }
            } else {
                $containment[$key] = $value;
            }
        }

        $allClauses = [];
        $allParams = [];

        if (count($containment) > 0) {
            $allClauses[] = 'data @> ?::jsonb';
            $allParams[] = json_encode(self::expandDotKeys($containment));
        }

        array_push($allClauses, ...$opClauses);
        array_push($allParams, ...$params);

        return [implode(' AND ', $allClauses), $allParams];
    }

    private static function hasOperators(array $value): bool
    {
        foreach (array_keys($value) as $k) {
            if (is_string($k) && str_starts_with($k, '$')) {
                return true;
            }
        }
        return false;
    }

    private static function ensureCollection(\PDO $pdo, string $collection): void
    {
        $pdo->exec(
            "CREATE TABLE IF NOT EXISTS {$collection} ("
            . "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), "
            . "data JSONB NOT NULL, "
            . "created_at TIMESTAMPTZ DEFAULT NOW())"
        );
    }

    public static function docInsert(\PDO $pdo, string $collection, array $document): array
    {
        self::validateIdentifier($collection);
        self::ensureCollection($pdo, $collection);
        $stmt = $pdo->prepare(
            "INSERT INTO {$collection} (data) VALUES (?::jsonb) RETURNING _id, data, created_at"
        );
        $stmt->execute([json_encode($document)]);
        return $stmt->fetch(\PDO::FETCH_ASSOC);
    }

    public static function docInsertMany(\PDO $pdo, string $collection, array $documents): array
    {
        self::validateIdentifier($collection);
        self::ensureCollection($pdo, $collection);
        $placeholders = implode(', ', array_map(fn() => '(?::jsonb)', $documents));
        $params = array_map(fn($d) => json_encode($d), $documents);
        $stmt = $pdo->prepare(
            "INSERT INTO {$collection} (data) VALUES {$placeholders} RETURNING _id, data, created_at"
        );
        $stmt->execute($params);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function docFind(
        \PDO $pdo,
        string $collection,
        ?array $filter = null,
        ?array $sort = null,
        ?int $limit = null,
        ?int $skip = null,
    ): array {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
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
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function docFindOne(\PDO $pdo, string $collection, ?array $filter = null): ?array
    {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
        $sql = "SELECT _id, data, created_at FROM {$collection}";
        if ($clause !== '') {
            $sql .= " WHERE {$clause}";
        }
        $sql .= " LIMIT 1";
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        return $row !== false ? $row : null;
    }

    public static function docUpdate(\PDO $pdo, string $collection, array $filter, array $update): int
    {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $allParams = [json_encode($update), ...$params];
        $stmt = $pdo->prepare(
            "UPDATE {$collection} SET data = data || ?::jsonb WHERE {$where}"
        );
        $stmt->execute($allParams);
        return $stmt->rowCount();
    }

    public static function docUpdateOne(\PDO $pdo, string $collection, array $filter, array $update): int
    {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $allParams = [...$params, json_encode($update)];
        $stmt = $pdo->prepare(
            "WITH target AS ("
            . "SELECT _id FROM {$collection} WHERE {$where} LIMIT 1"
            . ") UPDATE {$collection} SET data = data || ?::jsonb "
            . "FROM target WHERE {$collection}._id = target._id"
        );
        $stmt->execute($allParams);
        return $stmt->rowCount();
    }

    public static function docDelete(\PDO $pdo, string $collection, array $filter): int
    {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $stmt = $pdo->prepare(
            "DELETE FROM {$collection} WHERE {$where}"
        );
        $stmt->execute($params);
        return $stmt->rowCount();
    }

    public static function docDeleteOne(\PDO $pdo, string $collection, array $filter): int
    {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
        $where = $clause !== '' ? $clause : 'TRUE';
        $stmt = $pdo->prepare(
            "WITH target AS ("
            . "SELECT _id FROM {$collection} WHERE {$where} LIMIT 1"
            . ") DELETE FROM {$collection} USING target WHERE {$collection}._id = target._id"
        );
        $stmt->execute($params);
        return $stmt->rowCount();
    }

    public static function docCount(\PDO $pdo, string $collection, ?array $filter = null): int
    {
        self::validateIdentifier($collection);
        [$clause, $params] = self::buildFilter($filter);
        $sql = "SELECT COUNT(*) FROM {$collection}";
        if ($clause !== '') {
            $sql .= " WHERE {$clause}";
        }
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        return (int) $stmt->fetchColumn();
    }

    public static function docAggregate(\PDO $pdo, string $collection, array $pipeline): array
    {
        self::validateIdentifier($collection);

        if (empty($pipeline)) {
            return [];
        }

        $supportedStages = ['$match', '$group', '$sort', '$limit', '$skip', '$project', '$unwind', '$lookup'];

        // First pass: collect stages
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
                case '$match':
                    $matchStage = $stageValue;
                    break;
                case '$group':
                    $groupStage = $stageValue;
                    break;
                case '$sort':
                    $sortStage = $stageValue;
                    break;
                case '$limit':
                    $limitValue = $stageValue;
                    break;
                case '$skip':
                    $offsetValue = $stageValue;
                    break;
                case '$project':
                    $projectStage = $stageValue;
                    break;
                case '$unwind':
                    $unwindStage = $stageValue;
                    break;
                case '$lookup':
                    $lookupStages[] = $stageValue;
                    break;
            }
        }

        // Validate $limit / $skip
        if ($limitValue !== null && (!is_int($limitValue) || $limitValue < 0)) {
            throw new \InvalidArgumentException('$limit must be a non-negative integer');
        }
        if ($offsetValue !== null && (!is_int($offsetValue) || $offsetValue < 0)) {
            throw new \InvalidArgumentException('$skip must be a non-negative integer');
        }

        // Parse $unwind
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

        // Build $lookup subqueries
        $lookupSqls = [];
        foreach ($lookupStages as $lk) {
            $from = $lk['from'] ?? null;
            $localField = $lk['localField'] ?? null;
            $foreignField = $lk['foreignField'] ?? null;
            $asField = $lk['as'] ?? null;

            if ($from === null || $localField === null || $foreignField === null || $asField === null) {
                throw new \InvalidArgumentException('$lookup requires from, localField, foreignField, and as');
            }

            self::validateIdentifier($from);
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

        // Build WHERE clause
        $whereParams = [];
        $where = '';
        if ($matchStage !== null) {
            if (!is_array($matchStage)) {
                throw new \InvalidArgumentException('$match stage value must be an array');
            }
            [$matchClause, $matchParams] = self::buildFilter($matchStage);
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
            // $project stage: select specific fields
            if (!is_array($projectStage)) {
                throw new \InvalidArgumentException('$project stage value must be an array');
            }

            $projectParts = [];

            foreach ($projectStage as $key => $val) {
                if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $key)) {
                    throw new \InvalidArgumentException("Invalid field name: {$key}");
                }

                if ($val === 0 || $val === false) {
                    // Exclusion — skip
                    continue;
                }

                if ($val === 1 || $val === true) {
                    // Inclusion
                    if ($key === '_id') {
                        $projectParts[] = "_id";
                    } elseif ($groupStage !== null && isset($accumulators[$key])) {
                        $projectParts[] = $key;
                    } else {
                        $projectParts[] = self::resolveFieldRef('$' . $key, $unwindMap) . " AS {$key}";
                    }
                } elseif (is_string($val) && str_starts_with($val, '$')) {
                    // Rename: {alias: "$field"}
                    $projectParts[] = self::resolveFieldRef($val, $unwindMap) . " AS {$key}";
                } else {
                    throw new \InvalidArgumentException("Unsupported \$project value for {$key}: {$val}");
                }
            }

            // Handle _id: excluded explicitly, or included by default
            if (isset($projectStage['_id']) && ($projectStage['_id'] === 0 || $projectStage['_id'] === false)) {
                // _id excluded, do nothing
            } elseif (!isset($projectStage['_id'])) {
                // _id included by default
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
                // No group key — global aggregate
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
                    $resolved = self::resolveFieldRef($ref, $unwindMap);
                    $objParts[] = "'{$alias}', {$resolved}";
                    $groupByCols[] = $resolved;
                }
                $selectFields[] = "json_build_object(" . implode(', ', $objParts) . ") AS _id";
                $groupBy = "GROUP BY " . implode(', ', $groupByCols);
            } else {
                if (!is_string($groupId) || $groupId === '' || $groupId[0] !== '$') {
                    throw new \InvalidArgumentException('$group _id must be null, a "$field" reference, or an associative array');
                }
                $resolved = self::resolveFieldRef($groupId, $unwindMap);
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
                            $resolved = self::resolveFieldRef($opValue, $unwindMap);
                            $selectFields[] = "SUM(({$resolved})::numeric) AS {$alias}";
                        }
                        break;

                    case '$avg':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$avg");
                        }
                        $resolved = self::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "AVG(({$resolved})::numeric) AS {$alias}";
                        break;

                    case '$min':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$min");
                        }
                        $resolved = self::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "MIN(({$resolved})::numeric) AS {$alias}";
                        break;

                    case '$max':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$max");
                        }
                        $resolved = self::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "MAX(({$resolved})::numeric) AS {$alias}";
                        break;

                    case '$count':
                        $selectFields[] = "COUNT(*) AS {$alias}";
                        break;

                    case '$push':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$push");
                        }
                        $resolved = self::resolveFieldRef($opValue, $unwindMap);
                        $selectFields[] = "array_agg({$resolved}) AS {$alias}";
                        break;

                    case '$addToSet':
                        if (!is_string($opValue) || $opValue === '' || $opValue[0] !== '$') {
                            throw new \InvalidArgumentException("Invalid field reference in \$addToSet");
                        }
                        $resolved = self::resolveFieldRef($opValue, $unwindMap);
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
            // No $project, no $group — default columns
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

        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public static function docCreateIndex(\PDO $pdo, string $collection, ?array $keys = null): void
    {
        self::validateIdentifier($collection);
        if ($keys === null || count($keys) === 0) {
            $pdo->exec(
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
            $pdo->exec(
                "CREATE INDEX IF NOT EXISTS {$collection}_{$safeName}_idx "
                . "ON {$collection} ((data->>'{$key}') {$order})"
            );
        }
    }

    // ========================================================================
    // Change Streams (Watch)
    // ========================================================================

    public static function docWatch(\PDO $pdo, string $collection, ?callable $callback = null): void
    {
        self::validateIdentifier($collection);
        $channel = "{$collection}_changes";
        $funcName = "{$collection}_notify_fn";
        $triggerName = "{$collection}_notify_trg";

        $pdo->exec("
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

        $pdo->exec("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");

        $pdo->exec(
            "CREATE TRIGGER {$triggerName} "
            . "AFTER INSERT OR UPDATE OR DELETE ON {$collection} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );

        $pdo->exec("LISTEN {$channel}");
    }

    public static function docUnwatch(\PDO $pdo, string $collection): void
    {
        self::validateIdentifier($collection);
        $channel = "{$collection}_changes";
        $funcName = "{$collection}_notify_fn";
        $triggerName = "{$collection}_notify_trg";

        $pdo->exec("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $pdo->exec("DROP FUNCTION IF EXISTS {$funcName}()");
        $pdo->exec("UNLISTEN {$channel}");
    }

    // ========================================================================
    // TTL Indexes
    // ========================================================================

    public static function docCreateTtlIndex(\PDO $pdo, string $collection, int $expireAfterSeconds, string $field = 'created_at'): void
    {
        self::validateIdentifier($collection);
        self::validateIdentifier($field);
        if ($expireAfterSeconds <= 0) {
            throw new \InvalidArgumentException('expireAfterSeconds must be a positive integer');
        }

        $idxName = "{$collection}_ttl_idx";
        $funcName = "{$collection}_ttl_fn";
        $triggerName = "{$collection}_ttl_trg";

        $pdo->exec("CREATE INDEX IF NOT EXISTS {$idxName} ON {$collection} ({$field})");

        $pdo->exec("
            CREATE OR REPLACE FUNCTION {$funcName}()
            RETURNS TRIGGER LANGUAGE plpgsql AS \$\$
            BEGIN
                DELETE FROM {$collection} WHERE {$field} < NOW() - INTERVAL '{$expireAfterSeconds} seconds';
                RETURN NEW;
            END;
            \$\$
        ");

        $pdo->exec("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");

        $pdo->exec(
            "CREATE TRIGGER {$triggerName} "
            . "BEFORE INSERT ON {$collection} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );
    }

    public static function docRemoveTtlIndex(\PDO $pdo, string $collection): void
    {
        self::validateIdentifier($collection);

        $idxName = "{$collection}_ttl_idx";
        $funcName = "{$collection}_ttl_fn";
        $triggerName = "{$collection}_ttl_trg";

        $pdo->exec("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $pdo->exec("DROP FUNCTION IF EXISTS {$funcName}()");
        $pdo->exec("DROP INDEX IF EXISTS {$idxName}");
    }

    // ========================================================================
    // Capped Collections
    // ========================================================================

    public static function docCreateCapped(\PDO $pdo, string $collection, int $maxDocuments): void
    {
        self::validateIdentifier($collection);
        if ($maxDocuments <= 0) {
            throw new \InvalidArgumentException('maxDocuments must be a positive integer');
        }

        self::ensureCollection($pdo, $collection);

        $funcName = "{$collection}_cap_fn";
        $triggerName = "{$collection}_cap_trg";

        $pdo->exec("
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

        $pdo->exec("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");

        $pdo->exec(
            "CREATE TRIGGER {$triggerName} "
            . "AFTER INSERT ON {$collection} "
            . "FOR EACH ROW EXECUTE FUNCTION {$funcName}()"
        );
    }

    public static function docRemoveCap(\PDO $pdo, string $collection): void
    {
        self::validateIdentifier($collection);

        $funcName = "{$collection}_cap_fn";
        $triggerName = "{$collection}_cap_trg";

        $pdo->exec("DROP TRIGGER IF EXISTS {$triggerName} ON {$collection}");
        $pdo->exec("DROP FUNCTION IF EXISTS {$funcName}()");
    }
}
