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
}
