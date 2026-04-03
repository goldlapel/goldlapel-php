<?php

namespace GoldLapel;

class Utils
{
    public static function publish(\PDO $pdo, string $channel, string $message): void
    {
        $stmt = $pdo->prepare("SELECT pg_notify(?, ?)");
        $stmt->execute([$channel, $message]);
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
