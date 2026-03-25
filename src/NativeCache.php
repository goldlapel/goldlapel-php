<?php

namespace GoldLapel;

class NativeCache
{
    const DDL_SENTINEL = '__ddl__';

    private const TX_START = '/^\s*(BEGIN|START\s+TRANSACTION)\b/i';
    private const TX_END = '/^\s*(COMMIT|ROLLBACK|END)\b/i';
    private const TABLE_PATTERN = '/\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)/i';

    private const SQL_KEYWORDS = [
        'select', 'from', 'where', 'and', 'or', 'not', 'in', 'exists',
        'between', 'like', 'is', 'null', 'true', 'false', 'as', 'on',
        'left', 'right', 'inner', 'outer', 'cross', 'full', 'natural',
        'group', 'order', 'having', 'limit', 'offset', 'union', 'intersect',
        'except', 'all', 'distinct', 'lateral', 'values',
    ];

    private array $cache = [];
    private array $tableIndex = [];
    private array $accessOrder = [];
    private int $counter = 0;
    private int $maxEntries;
    private bool $enabled;
    private bool $invalidationConnected = false;
    private int $invalidationPort = 0;

    public int $statsHits = 0;
    public int $statsMisses = 0;
    public int $statsInvalidations = 0;

    private static ?self $instance = null;

    public function __construct(?int $maxEntries = null, ?bool $enabled = null)
    {
        $this->maxEntries = $maxEntries ?? (int) (getenv('GOLDLAPEL_NATIVE_CACHE_SIZE') ?: '32768');
        $envEnabled = getenv('GOLDLAPEL_NATIVE_CACHE');
        $this->enabled = $enabled ?? ($envEnabled === false || strtolower($envEnabled) !== 'false');
    }

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
        return $this->invalidationConnected;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function size(): int
    {
        return count($this->cache);
    }

    // --- Cache operations ---

    public function get(string $sql, ?array $params = null): ?array
    {
        if (!$this->enabled || !$this->invalidationConnected) {
            return null;
        }
        $key = self::makeKey($sql, $params);
        if ($key === null) {
            return null;
        }
        if (isset($this->cache[$key])) {
            $this->counter++;
            $this->accessOrder[$key] = $this->counter;
            $this->statsHits++;
            return $this->cache[$key];
        }
        $this->statsMisses++;
        return null;
    }

    public function put(string $sql, ?array $params, array $rows, ?array $columns): void
    {
        if (!$this->enabled || !$this->invalidationConnected) {
            return;
        }
        $key = self::makeKey($sql, $params);
        if ($key === null) {
            return;
        }
        $tables = self::extractTables($sql);
        if (!isset($this->cache[$key]) && count($this->cache) >= $this->maxEntries) {
            $this->evictOne();
        }
        $this->cache[$key] = ['rows' => $rows, 'columns' => $columns, 'tables' => $tables];
        $this->counter++;
        $this->accessOrder[$key] = $this->counter;
        foreach ($tables as $table) {
            if (!isset($this->tableIndex[$table])) {
                $this->tableIndex[$table] = [];
            }
            $this->tableIndex[$table][$key] = true;
        }
    }

    public function invalidateTable(string $table): void
    {
        $table = strtolower($table);
        $keys = $this->tableIndex[$table] ?? null;
        if ($keys === null) {
            return;
        }
        unset($this->tableIndex[$table]);
        foreach (array_keys($keys) as $key) {
            $entry = $this->cache[$key] ?? null;
            unset($this->cache[$key]);
            unset($this->accessOrder[$key]);
            if ($entry !== null) {
                foreach ($entry['tables'] as $otherTable) {
                    if ($otherTable !== $table && isset($this->tableIndex[$otherTable])) {
                        unset($this->tableIndex[$otherTable][$key]);
                        if (empty($this->tableIndex[$otherTable])) {
                            unset($this->tableIndex[$otherTable]);
                        }
                    }
                }
            }
        }
        $this->statsInvalidations += count($keys);
    }

    public function invalidateAll(): void
    {
        $count = count($this->cache);
        $this->cache = [];
        $this->tableIndex = [];
        $this->accessOrder = [];
        $this->statsInvalidations += $count;
    }

    // --- Invalidation connection ---

    public function connectInvalidation(int $port): void
    {
        $this->invalidationPort = $port;
        $sockPath = "/tmp/goldlapel-{$port}.sock";

        $sock = null;
        try {
            if (PHP_OS_FAMILY !== 'Windows' && file_exists($sockPath)) {
                $sock = @stream_socket_client("unix://{$sockPath}", $errno, $errstr, 1);
            } else {
                $sock = @stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $errstr, 1);
            }

            if ($sock === false) {
                return;
            }

            stream_set_blocking($sock, false);
            $this->invalidationConnected = true;

            // Non-blocking read of any pending signals
            $this->drainSignals($sock);
            fclose($sock);
        } catch (\Throwable $e) {
            $this->invalidationConnected = false;
            if ($sock && is_resource($sock)) {
                fclose($sock);
            }
        }
    }

    public function connectInvalidationPersistent(int $port): void
    {
        $this->invalidationPort = $port;
        $this->invalidationConnected = false;
        $sockPath = "/tmp/goldlapel-{$port}.sock";

        $sock = null;
        try {
            if (PHP_OS_FAMILY !== 'Windows' && file_exists($sockPath)) {
                $sock = @stream_socket_client("unix://{$sockPath}", $errno, $errstr, 1);
            } else {
                $sock = @stream_socket_client("tcp://127.0.0.1:{$port}", $errno, $errstr, 1);
            }

            if ($sock === false) {
                return;
            }

            stream_set_blocking($sock, false);
            $this->invalidationConnected = true;

            // For long-running processes: read signals using stream_select
            $this->readSignalsLoop($sock);
        } catch (\Throwable $e) {
            if ($this->invalidationConnected) {
                $this->invalidationConnected = false;
                $this->invalidateAll();
            }
        } finally {
            if ($sock && is_resource($sock)) {
                fclose($sock);
            }
        }
    }

    public function setConnected(bool $connected): void
    {
        $this->invalidationConnected = $connected;
    }

    public function processSignal(string $line): void
    {
        if (str_starts_with($line, 'I:')) {
            $table = trim(substr($line, 2));
            if ($table === '*') {
                $this->invalidateAll();
            } else {
                $this->invalidateTable($table);
            }
        }
    }

    // --- SQL parsing (static) ---

    public static function makeKey(string $sql, ?array $params = null): ?string
    {
        if ($params === null || empty($params)) {
            return $sql . "\0null";
        }
        return $sql . "\0" . serialize($params);
    }

    public static function detectWrite(string $sql): ?string
    {
        $trimmed = trim($sql);
        $tokens = preg_split('/\s+/', $trimmed);
        if (empty($tokens) || $tokens[0] === '') {
            return null;
        }
        $first = strtoupper($tokens[0]);

        switch ($first) {
            case 'INSERT':
                if (count($tokens) < 3 || strtoupper($tokens[1]) !== 'INTO') {
                    return null;
                }
                return self::bareTable($tokens[2]);
            case 'UPDATE':
                if (count($tokens) < 2) {
                    return null;
                }
                return self::bareTable($tokens[1]);
            case 'DELETE':
                if (count($tokens) < 3 || strtoupper($tokens[1]) !== 'FROM') {
                    return null;
                }
                return self::bareTable($tokens[2]);
            case 'TRUNCATE':
                if (count($tokens) < 2) {
                    return null;
                }
                if (strtoupper($tokens[1]) === 'TABLE') {
                    if (count($tokens) < 3) {
                        return null;
                    }
                    return self::bareTable($tokens[2]);
                }
                return self::bareTable($tokens[1]);
            case 'CREATE':
            case 'ALTER':
            case 'DROP':
                return self::DDL_SENTINEL;
            case 'COPY':
                if (count($tokens) < 2) {
                    return null;
                }
                $raw = $tokens[1];
                if (str_starts_with($raw, '(')) {
                    return null;
                }
                $tablePart = explode('(', $raw)[0];
                for ($i = 2; $i < count($tokens); $i++) {
                    $upper = strtoupper($tokens[$i]);
                    if ($upper === 'FROM') {
                        return self::bareTable($tablePart);
                    }
                    if ($upper === 'TO') {
                        return null;
                    }
                }
                return null;
            case 'WITH':
                $restUpper = strtoupper(substr($trimmed, strlen($tokens[0])));
                foreach (preg_split('/\s+/', $restUpper) as $token) {
                    $word = ltrim($token, '(');
                    if (in_array($word, ['INSERT', 'UPDATE', 'DELETE'], true)) {
                        return self::DDL_SENTINEL;
                    }
                }
                return null;
            default:
                return null;
        }
    }

    public static function bareTable(string $raw): string
    {
        $table = explode('(', $raw)[0];
        $parts = explode('.', $table);
        $table = end($parts);
        return strtolower($table);
    }

    public static function extractTables(string $sql): array
    {
        $tables = [];
        $keywords = array_flip(self::SQL_KEYWORDS);
        if (preg_match_all(self::TABLE_PATTERN, $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $match) {
                $table = strtolower($match[2]);
                if (!isset($keywords[$table])) {
                    $tables[$table] = true;
                }
            }
        }
        return array_keys($tables);
    }

    public static function isTxStart(string $sql): bool
    {
        return (bool) preg_match(self::TX_START, $sql);
    }

    public static function isTxEnd(string $sql): bool
    {
        return (bool) preg_match(self::TX_END, $sql);
    }

    // --- Private helpers ---

    private function evictOne(): void
    {
        if (empty($this->accessOrder)) {
            return;
        }
        $lruKey = array_keys($this->accessOrder, min($this->accessOrder))[0];
        $entry = $this->cache[$lruKey] ?? null;
        unset($this->cache[$lruKey]);
        unset($this->accessOrder[$lruKey]);
        if ($entry !== null) {
            foreach ($entry['tables'] as $table) {
                if (isset($this->tableIndex[$table])) {
                    unset($this->tableIndex[$table][$lruKey]);
                    if (empty($this->tableIndex[$table])) {
                        unset($this->tableIndex[$table]);
                    }
                }
            }
        }
    }

    private function drainSignals($sock): void
    {
        $buf = '';
        while (true) {
            $read = [$sock];
            $write = null;
            $except = null;
            $ready = @stream_select($read, $write, $except, 0, 0);
            if ($ready === false || $ready === 0) {
                break;
            }
            $data = @fread($sock, 4096);
            if ($data === false || $data === '') {
                break;
            }
            $buf .= $data;
            while (($pos = strpos($buf, "\n")) !== false) {
                $line = substr($buf, 0, $pos);
                $buf = substr($buf, $pos + 1);
                $this->processSignal($line);
            }
        }
    }

    private function readSignalsLoop($sock): void
    {
        $buf = '';
        while ($this->invalidationConnected) {
            $read = [$sock];
            $write = null;
            $except = null;
            $ready = @stream_select($read, $write, $except, 30);
            if ($ready === false) {
                break;
            }
            if ($ready === 0) {
                break; // timeout
            }
            $data = @fread($sock, 4096);
            if ($data === false || $data === '') {
                break;
            }
            $buf .= $data;
            while (($pos = strpos($buf, "\n")) !== false) {
                $line = substr($buf, 0, $pos);
                $buf = substr($buf, $pos + 1);
                $this->processSignal($line);
            }
        }
    }
}
