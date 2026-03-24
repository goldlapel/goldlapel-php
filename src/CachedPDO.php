<?php

namespace GoldLapel;

class CachedPDO
{
    private \PDO $pdo;
    private NativeCache $cache;
    private bool $inTransaction = false;

    public function __construct(\PDO $pdo, NativeCache $cache)
    {
        $this->pdo = $pdo;
        $this->cache = $cache;
    }

    public function getWrappedPDO(): \PDO
    {
        return $this->pdo;
    }

    public function getCache(): NativeCache
    {
        return $this->cache;
    }

    public function query(string $sql, ...$args): CachedPDOStatement|false
    {
        // Transaction tracking
        if (NativeCache::isTxStart($sql)) {
            $this->inTransaction = true;
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $sql, null, $this->inTransaction) : false;
        }
        if (NativeCache::isTxEnd($sql)) {
            $this->inTransaction = false;
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $sql, null, $this->inTransaction) : false;
        }

        // Write detection + self-invalidation
        $writeTable = NativeCache::detectWrite($sql);
        if ($writeTable !== null) {
            if ($writeTable === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                $this->cache->invalidateTable($writeTable);
            }
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $sql, null, $this->inTransaction) : false;
        }

        // Inside transaction: bypass cache
        if ($this->inTransaction) {
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $sql, null, true) : false;
        }

        // Read path: check L1 cache
        $entry = $this->cache->get($sql, null);
        if ($entry !== null) {
            return CachedPDOStatement::fromCache($entry, $this->cache, $sql);
        }

        // Cache miss: execute for real
        $stmt = $this->pdo->query($sql, ...$args);
        if ($stmt === false) {
            return false;
        }

        // Cache the result
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $columns = !empty($rows) ? array_keys($rows[0]) : [];
        $this->cache->put($sql, null, $rows, $columns);

        return CachedPDOStatement::fromCache(
            ['rows' => $rows, 'columns' => $columns, 'tables' => NativeCache::extractTables($sql)],
            $this->cache,
            $sql
        );
    }

    public function prepare(string $sql, array $options = []): CachedPDOStatement
    {
        $realStmt = $this->pdo->prepare($sql, $options);
        return new CachedPDOStatement($realStmt, $this->cache, $sql, null, $this->inTransaction);
    }

    public function exec(string $sql): int|false
    {
        // Write detection
        $writeTable = NativeCache::detectWrite($sql);
        if ($writeTable !== null) {
            if ($writeTable === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                $this->cache->invalidateTable($writeTable);
            }
        }

        // Transaction tracking
        if (NativeCache::isTxStart($sql)) {
            $this->inTransaction = true;
        } elseif (NativeCache::isTxEnd($sql)) {
            $this->inTransaction = false;
        }

        return $this->pdo->exec($sql);
    }

    public function beginTransaction(): bool
    {
        $this->inTransaction = true;
        return $this->pdo->beginTransaction();
    }

    public function commit(): bool
    {
        $this->inTransaction = false;
        return $this->pdo->commit();
    }

    public function rollBack(): bool
    {
        $this->inTransaction = false;
        return $this->pdo->rollBack();
    }

    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    public function lastInsertId(?string $name = null): string|false
    {
        return $this->pdo->lastInsertId($name);
    }

    public function quote(string $string, int $type = \PDO::PARAM_STR): string|false
    {
        return $this->pdo->quote($string, $type);
    }

    public function setAttribute(int $attribute, mixed $value): bool
    {
        return $this->pdo->setAttribute($attribute, $value);
    }

    public function getAttribute(int $attribute): mixed
    {
        return $this->pdo->getAttribute($attribute);
    }

    public function errorCode(): ?string
    {
        return $this->pdo->errorCode();
    }

    public function errorInfo(): array
    {
        return $this->pdo->errorInfo();
    }

    public function __call(string $method, array $args): mixed
    {
        return $this->pdo->$method(...$args);
    }
}

class CachedPDOStatement implements \IteratorAggregate
{
    private ?\PDOStatement $realStmt;
    private NativeCache $cache;
    private string $sql;
    private ?array $params;
    private bool $inTransaction;

    private ?array $cachedRows = null;
    private ?array $cachedColumns = null;
    private int $fetchIndex = 0;
    private bool $fromCache = false;

    public function __construct(
        ?\PDOStatement $realStmt,
        NativeCache $cache,
        string $sql,
        ?array $params = null,
        bool $inTransaction = false,
    ) {
        $this->realStmt = $realStmt;
        $this->cache = $cache;
        $this->sql = $sql;
        $this->params = $params;
        $this->inTransaction = $inTransaction;
    }

    public static function fromCache(array $entry, NativeCache $cache, string $sql): self
    {
        $stmt = new self(null, $cache, $sql);
        $stmt->cachedRows = $entry['rows'];
        $stmt->cachedColumns = $entry['columns'];
        $stmt->fetchIndex = 0;
        $stmt->fromCache = true;
        return $stmt;
    }

    public function execute(?array $params = null): bool
    {
        $effectiveParams = $params ?? $this->params;
        $this->cachedRows = null;
        $this->cachedColumns = null;
        $this->fetchIndex = 0;
        $this->fromCache = false;

        // Transaction tracking
        if (NativeCache::isTxStart($this->sql)) {
            $this->inTransaction = true;
            return $this->realStmt->execute($params);
        }
        if (NativeCache::isTxEnd($this->sql)) {
            $this->inTransaction = false;
            return $this->realStmt->execute($params);
        }

        // Write detection + self-invalidation
        $writeTable = NativeCache::detectWrite($this->sql);
        if ($writeTable !== null) {
            if ($writeTable === NativeCache::DDL_SENTINEL) {
                $this->cache->invalidateAll();
            } else {
                $this->cache->invalidateTable($writeTable);
            }
            return $this->realStmt->execute($params);
        }

        // Inside transaction: bypass cache
        if ($this->inTransaction) {
            return $this->realStmt->execute($params);
        }

        // Read path: check L1 cache
        $entry = $this->cache->get($this->sql, $effectiveParams);
        if ($entry !== null) {
            $this->cachedRows = $entry['rows'];
            $this->cachedColumns = $entry['columns'];
            $this->fetchIndex = 0;
            $this->fromCache = true;
            return true;
        }

        // Cache miss: execute for real
        $result = $this->realStmt->execute($params);
        if (!$result) {
            return false;
        }

        // Try to cache the result
        if ($this->realStmt->columnCount() > 0) {
            try {
                $rows = $this->realStmt->fetchAll(\PDO::FETCH_ASSOC);
                $columns = !empty($rows) ? array_keys($rows[0]) : [];
                $this->cache->put($this->sql, $effectiveParams, $rows, $columns);
                $this->cachedRows = $rows;
                $this->cachedColumns = $columns;
                $this->fetchIndex = 0;
                $this->fromCache = true;
            } catch (\Throwable $e) {
                // If caching fails, fall through to real statement
            }
        }

        return $result;
    }

    public function fetch(int $mode = \PDO::FETCH_DEFAULT, ...$args): mixed
    {
        if ($this->cachedRows !== null) {
            if ($this->fetchIndex >= count($this->cachedRows)) {
                return false;
            }
            $row = $this->cachedRows[$this->fetchIndex];
            $this->fetchIndex++;

            if ($mode === \PDO::FETCH_NUM || $mode === \PDO::FETCH_BOTH) {
                $numRow = array_values($row);
                if ($mode === \PDO::FETCH_BOTH) {
                    return array_merge($numRow, $row);
                }
                return $numRow;
            }
            return $row; // FETCH_ASSOC or FETCH_DEFAULT
        }
        return $this->realStmt?->fetch($mode, ...$args) ?? false;
    }

    public function fetchAll(int $mode = \PDO::FETCH_DEFAULT, ...$args): array
    {
        if ($this->cachedRows !== null) {
            $remaining = array_slice($this->cachedRows, $this->fetchIndex);
            $this->fetchIndex = count($this->cachedRows);

            if ($mode === \PDO::FETCH_NUM) {
                return array_map('array_values', $remaining);
            }
            if ($mode === \PDO::FETCH_BOTH) {
                return array_map(function ($row) {
                    return array_merge(array_values($row), $row);
                }, $remaining);
            }
            return $remaining; // FETCH_ASSOC or FETCH_DEFAULT
        }
        return $this->realStmt?->fetchAll($mode, ...$args) ?? [];
    }

    public function fetchColumn(int $column = 0): mixed
    {
        if ($this->cachedRows !== null) {
            if ($this->fetchIndex >= count($this->cachedRows)) {
                return false;
            }
            $row = $this->cachedRows[$this->fetchIndex];
            $this->fetchIndex++;
            $values = array_values($row);
            return $values[$column] ?? false;
        }
        return $this->realStmt?->fetchColumn($column) ?? false;
    }

    public function rowCount(): int
    {
        if ($this->cachedRows !== null) {
            return count($this->cachedRows);
        }
        return $this->realStmt?->rowCount() ?? 0;
    }

    public function columnCount(): int
    {
        if ($this->cachedColumns !== null) {
            return count($this->cachedColumns);
        }
        return $this->realStmt?->columnCount() ?? 0;
    }

    public function closeCursor(): bool
    {
        $this->cachedRows = null;
        $this->cachedColumns = null;
        $this->fetchIndex = 0;
        return $this->realStmt?->closeCursor() ?? true;
    }

    public function bindValue(string|int $param, mixed $value, int $type = \PDO::PARAM_STR): bool
    {
        if ($this->params === null) {
            $this->params = [];
        }
        $this->params[$param] = $value;
        return $this->realStmt?->bindValue($param, $value, $type) ?? false;
    }

    public function bindParam(
        string|int $param,
        mixed &$var,
        int $type = \PDO::PARAM_STR,
        int $maxLength = 0,
        mixed $driverOptions = null,
    ): bool {
        return $this->realStmt?->bindParam($param, $var, $type, $maxLength, $driverOptions) ?? false;
    }

    public function getIterator(): \Traversable
    {
        if ($this->cachedRows !== null) {
            return new \ArrayIterator(array_slice($this->cachedRows, $this->fetchIndex));
        }
        return $this->realStmt ?? new \ArrayIterator([]);
    }

    public function __call(string $method, array $args): mixed
    {
        if ($this->realStmt === null) {
            throw new \BadMethodCallException("Cannot call {$method} on a cached-only statement");
        }
        return $this->realStmt->$method(...$args);
    }
}
