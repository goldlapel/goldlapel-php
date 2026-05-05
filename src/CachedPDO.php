<?php

namespace GoldLapel;

class CachedPDO extends \PDO
{
    private \PDO $pdo;
    private NativeCache $cache;
    /**
     * Per-connection unsafe-GUC state tracker. Each CachedPDO has its own
     * — sharing it across connections would let one connection's `SET
     * app.user_id` poison another's cache reads. See ConnectionGucState
     * for the security rationale.
     */
    private ConnectionGucState $gucState;
    private bool $inTransaction = false;

    // Extends PDO so instanceof checks and type hints work.
    // We intentionally skip parent::__construct() to avoid opening a
    // second connection — all calls delegate to the wrapped $pdo.
    public function __construct(\PDO $pdo, NativeCache $cache)
    {
        // Do NOT call parent::__construct() — we delegate to $pdo instead.
        $this->pdo = $pdo;
        $this->cache = $cache;
        $this->gucState = new ConnectionGucState();
    }

    public function getWrappedPDO(): \PDO
    {
        return $this->pdo;
    }

    public function unwrap(): \PDO
    {
        return $this->pdo;
    }

    public function getCache(): NativeCache
    {
        return $this->cache;
    }

    public function getGucState(): ConnectionGucState
    {
        return $this->gucState;
    }

    public function query(string $sql, ...$args): CachedPDOStatement|false
    {
        // Observe unsafe-GUC SET / RESET before any other handling so the
        // state hash reflects the effective post-statement state when the
        // result is keyed (matters for `SET app.user_id = '7'; SELECT ...`
        // multi-statements). State lives on this CachedPDO so concurrent
        // connections never share gucState.
        $this->gucState->observeSql($sql);

        // Transaction tracking
        if (NativeCache::isTxStart($sql)) {
            $this->inTransaction = true;
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction) : false;
        }
        if (NativeCache::isTxEnd($sql)) {
            $this->inTransaction = false;
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction) : false;
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
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, $this->inTransaction) : false;
        }

        // Inside transaction: bypass cache
        if ($this->inTransaction) {
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $this->gucState, $sql, null, true) : false;
        }

        // Read path: check native cache, keyed on this connection's GUC fingerprint.
        $stateHash = $this->gucState->stateHash();
        $entry = $this->cache->get($sql, null, $stateHash);
        if ($entry !== null) {
            return CachedPDOStatement::fromCache($entry, $this->cache, $this->gucState, $sql);
        }

        // Cache miss: execute for real
        $stmt = $this->pdo->query($sql, ...$args);
        if ($stmt === false) {
            return false;
        }

        // Cache the result under this connection's state hash.
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $columns = !empty($rows) ? array_keys($rows[0]) : [];
        $this->cache->put($sql, null, $rows, $columns, $stateHash);

        return CachedPDOStatement::fromCache(
            ['rows' => $rows, 'columns' => $columns, 'tables' => NativeCache::extractTables($sql)],
            $this->cache,
            $this->gucState,
            $sql
        );
    }

    public function prepare(string $sql, array $options = []): CachedPDOStatement
    {
        $realStmt = $this->pdo->prepare($sql, $options);
        return new CachedPDOStatement($realStmt, $this->cache, $this->gucState, $sql, null, fn() => $this->inTransaction);
    }

    public function exec(string $sql): int|false
    {
        // Observe unsafe-GUC SET / RESET before write detection. Mirrors
        // query(); a `SET app.user_id = '7'` issued via exec() must
        // update this connection's state hash so subsequent reads on the
        // same CachedPDO key correctly.
        $this->gucState->observeSql($sql);

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

class CachedPDOStatement extends \PDOStatement
{
    private ?\PDOStatement $realStmt;
    private NativeCache $cache;
    /**
     * Per-connection GUC state, shared by-reference with the parent
     * CachedPDO. Statements observe SET / RESET so a `SET app.user_id = …`
     * issued through `prepare`+`execute` updates the connection's state,
     * not the statement's — that's what ConnectionGucState's
     * `observeSql()` does (the object holds the state).
     */
    private ConnectionGucState $gucState;
    private string $sql;
    private ?array $params;
    private \Closure $inTransaction;

    private ?array $cachedRows = null;
    private ?array $cachedColumns = null;
    private int $fetchIndex = 0;
    private bool $fromCache = false;

    public function __construct(
        ?\PDOStatement $realStmt,
        NativeCache $cache,
        ConnectionGucState $gucState,
        string $sql,
        ?array $params = null,
        bool|\Closure $inTransaction = false,
    ) {
        $this->realStmt = $realStmt;
        $this->cache = $cache;
        $this->gucState = $gucState;
        $this->sql = $sql;
        $this->params = $params;
        if ($inTransaction instanceof \Closure) {
            $this->inTransaction = $inTransaction;
        } else {
            $val = $inTransaction;
            $this->inTransaction = static function () use ($val) { return $val; };
        }
    }

    public static function fromCache(array $entry, NativeCache $cache, ConnectionGucState $gucState, string $sql): self
    {
        $stmt = new self(null, $cache, $gucState, $sql);
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

        // Observe unsafe-GUC SET / RESET. Prepared `SET app.user_id = $1`
        // is unusual but legal; observeSql() captures the placeholder
        // string `$1` as the recorded value, which won't match the
        // proxy's per-bind hash — at worst the wrapper sees cache misses
        // for that connection's reads. The proxy still keys correctly
        // because it sees the bound value on the wire, so this is
        // never a leak.
        $this->gucState->observeSql($this->sql);

        // Transaction tracking
        if (NativeCache::isTxStart($this->sql)) {
            return $this->realStmt->execute($params);
        }
        if (NativeCache::isTxEnd($this->sql)) {
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
        if (($this->inTransaction)()) {
            return $this->realStmt->execute($params);
        }

        // Read path: check native cache, keyed on this connection's GUC fingerprint.
        $stateHash = $this->gucState->stateHash();
        $entry = $this->cache->get($this->sql, $effectiveParams, $stateHash);
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
                $this->cache->put($this->sql, $effectiveParams, $rows, $columns, $stateHash);
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
        if ($this->params === null) {
            $this->params = [];
        }
        $this->params[$param] = $var;
        return $this->realStmt?->bindParam($param, $var, $type, $maxLength, $driverOptions) ?? false;
    }

    public function getIterator(): \Iterator
    {
        if ($this->cachedRows !== null) {
            return new \ArrayIterator(array_slice($this->cachedRows, $this->fetchIndex));
        }
        if ($this->realStmt !== null) {
            return new \ArrayIterator($this->realStmt->fetchAll(\PDO::FETCH_ASSOC));
        }
        return new \ArrayIterator([]);
    }

    public function __call(string $method, array $args): mixed
    {
        if ($this->realStmt === null) {
            throw new \BadMethodCallException("Cannot call {$method} on a cached-only statement");
        }
        return $this->realStmt->$method(...$args);
    }
}
