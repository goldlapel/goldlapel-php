<?php

// Define spy GoldLapel classes before the autoloader loads the real ones.
// This lets us test the service provider without needing the actual binary.

namespace GoldLapel {
    class NativeCache
    {
        const DDL_SENTINEL = '__ddl__';

        private static ?self $instance = null;
        private bool $connected = false;
        public int $statsHits = 0;
        public int $statsMisses = 0;
        public int $statsInvalidations = 0;

        public static function getInstance(): self
        {
            if (self::$instance === null) {
                self::$instance = new self();
            }
            return self::$instance;
        }

        public static function reset(): void
        {
            self::$instance = null;
        }

        public function isConnected(): bool
        {
            return $this->connected;
        }

        public function setConnected(bool $connected): void
        {
            $this->connected = $connected;
        }

        public function connectInvalidation(int $port): void
        {
            $this->connected = true;
        }

        public static function detectWrite(string $sql): ?string
        {
            $trimmed = trim($sql);
            $tokens = preg_split('/\s+/', $trimmed);
            if (empty($tokens) || $tokens[0] === '') {
                return null;
            }
            $first = strtoupper($tokens[0]);

            return match ($first) {
                'INSERT' => (count($tokens) >= 3 && strtoupper($tokens[1]) === 'INTO') ? strtolower($tokens[2]) : null,
                'UPDATE' => count($tokens) >= 2 ? strtolower($tokens[1]) : null,
                'DELETE' => (count($tokens) >= 3 && strtoupper($tokens[1]) === 'FROM') ? strtolower($tokens[2]) : null,
                'CREATE', 'ALTER', 'DROP' => self::DDL_SENTINEL,
                default => null,
            };
        }

        public function invalidateTable(string $table): void {}
        public function invalidateAll(): void {}
        public function get(string $sql, ?array $params = null): ?array { return null; }
        public function put(string $sql, ?array $params, array $rows, ?array $columns): void {}

        public static function extractTables(string $sql): array { return []; }
        public static function isTxStart(string $sql): bool { return false; }
        public static function isTxEnd(string $sql): bool { return false; }
    }

    class CachedPDO
    {
        private \PDO $pdo;
        private NativeCache $cache;

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

        public function prepare(string $sql, array $options = []): CachedPDOStatement
        {
            $realStmt = $this->pdo->prepare($sql, $options);
            return new CachedPDOStatement($realStmt, $this->cache, $sql);
        }

        public function query(string $sql, ...$args)
        {
            $stmt = $this->pdo->query($sql, ...$args);
            return $stmt !== false ? new CachedPDOStatement($stmt, $this->cache, $sql) : false;
        }

        public function exec(string $sql)
        {
            return $this->pdo->exec($sql);
        }

        public function __call(string $method, array $args)
        {
            return $this->pdo->$method(...$args);
        }
    }

    class CachedPDOStatement implements \IteratorAggregate
    {
        private ?\PDOStatement $realStmt;
        private NativeCache $cache;
        private string $sql;

        public function __construct(?\PDOStatement $realStmt, NativeCache $cache, string $sql)
        {
            $this->realStmt = $realStmt;
            $this->cache = $cache;
            $this->sql = $sql;
        }

        public function execute(?array $params = null): bool
        {
            return $this->realStmt->execute($params);
        }

        public function fetch(int $mode = \PDO::FETCH_DEFAULT, ...$args): mixed
        {
            return $this->realStmt->fetch($mode, ...$args);
        }

        public function fetchAll(int $mode = \PDO::FETCH_DEFAULT, ...$args): array
        {
            return $this->realStmt->fetchAll($mode, ...$args);
        }

        public function fetchColumn(int $column = 0): mixed
        {
            return $this->realStmt->fetchColumn($column);
        }

        public function rowCount(): int
        {
            return $this->realStmt->rowCount();
        }

        public function columnCount(): int
        {
            return $this->realStmt->columnCount();
        }

        public function bindValue(string|int $param, mixed $value, int $type = \PDO::PARAM_STR): bool
        {
            return $this->realStmt->bindValue($param, $value, $type);
        }

        public function bindParam(string|int $param, mixed &$var, int $type = \PDO::PARAM_STR, int $maxLength = 0, mixed $driverOptions = null): bool
        {
            return $this->realStmt->bindParam($param, $var, $type, $maxLength, $driverOptions);
        }

        public function closeCursor(): bool
        {
            return $this->realStmt->closeCursor();
        }

        public function getIterator(): \Traversable
        {
            return $this->realStmt ?? new \ArrayIterator([]);
        }

        public function __call(string $method, array $args): mixed
        {
            return $this->realStmt->$method(...$args);
        }
    }

    class GoldLapel
    {
        const DEFAULT_PORT = 7932;

        public static array $calls = [];
        public static array $wrapCalls = [];

        /** @var array<int, self> */
        public static array $liveInstances = [];

        public int $stopCalls = 0;
        private ?string $url = null;

        public static function reset(): void
        {
            self::$calls = [];
            self::$wrapCalls = [];
            self::$liveInstances = [];
            NativeCache::reset();
        }

        public static function start(string $upstream, array $options = []): self
        {
            self::$calls[] = [
                'upstream' => $upstream,
                'port' => $options['port'] ?? self::DEFAULT_PORT,
                'config' => $options['config'] ?? [],
                'extraArgs' => $options['extra_args'] ?? [],
            ];
            $instance = new self();
            $port = $options['port'] ?? self::DEFAULT_PORT;
            $instance->url = "postgresql://localhost:{$port}/db";
            self::$liveInstances[spl_object_id($instance)] = $instance;
            return $instance;
        }

        public static function startProxyOnly(string $upstream, array $options = []): self
        {
            $port = $options['port'] ?? self::DEFAULT_PORT;
            self::$calls[] = [
                'upstream' => $upstream,
                'port' => $port,
                'config' => $options['config'] ?? [],
                'extraArgs' => $options['extra_args'] ?? [],
            ];
            $instance = new self();
            $instance->url = "postgresql://localhost:{$port}/db";
            self::$liveInstances[spl_object_id($instance)] = $instance;
            return $instance;
        }

        public static function wrapPDOStatic(\PDO $pdo, int $invalidationPort): CachedPDO
        {
            self::$wrapCalls[] = compact('pdo', 'invalidationPort');

            $cache = NativeCache::getInstance();
            if (!$cache->isConnected()) {
                $cache->connectInvalidation($invalidationPort);
            }

            return new CachedPDO($pdo, $cache);
        }

        public function stop(): void
        {
            $this->stopCalls++;
            unset(self::$liveInstances[spl_object_id($this)]);
        }

        public function url(): ?string { return $this->url; }
        public static function cleanupAll(): void {}
    }
}

namespace {
    require __DIR__ . '/../../vendor/autoload.php';
}
