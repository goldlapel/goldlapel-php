<?php

namespace GoldLapel\Laravel;

use GoldLapel\CachedPDO;
use GoldLapel\GoldLapel;
use GoldLapel\NativeCache;
use Illuminate\Database\PostgresConnection;

class GoldLapelConnection extends PostgresConnection
{
    private ?CachedPDO $cachedPdo = null;
    private ?int $invalidationPort = null;

    public function setInvalidationPort(?int $port)
    {
        $this->invalidationPort = $port;
    }

    public function getCachedPDO()
    {
        if ($this->cachedPdo === null) {
            $pdo = $this->getPdo();
            $this->cachedPdo = GoldLapel::wrapPDO($pdo, $this->invalidationPort);
        }

        return $this->cachedPdo;
    }

    public function select($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) use ($useReadPdo) {
            if ($this->pretending()) {
                return [];
            }

            $cached = $this->getCachedPDO();
            $stmt = $cached->prepare($query);

            $prepared = $this->prepareBindings($bindings);
            foreach ($prepared as $key => $value) {
                $stmt->bindValue(
                    is_string($key) ? $key : $key + 1,
                    $value,
                    match (true) {
                        is_int($value) => \PDO::PARAM_INT,
                        is_bool($value) => \PDO::PARAM_BOOL,
                        is_null($value) => \PDO::PARAM_NULL,
                        default => \PDO::PARAM_STR,
                    }
                );
            }

            $stmt->execute();

            $rows = $stmt->fetchAll($this->fetchMode);

            // CachedPDOStatement returns FETCH_ASSOC arrays for cached results
            // regardless of fetch mode. Convert to objects when Laravel expects them.
            if ($this->fetchMode === \PDO::FETCH_OBJ && !empty($rows) && is_array($rows[0])) {
                return array_map(fn ($row) => (object) $row, $rows);
            }

            return $rows;
        });
    }

    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $this->notifyCacheOfWrite($query);

            $statement = $this->getPdo()->prepare($query);
            $this->bindValues($statement, $this->prepareBindings($bindings));
            $this->recordsHaveBeenModified();

            return $statement->execute();
        });
    }

    public function affectingStatement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            $this->notifyCacheOfWrite($query);

            $statement = $this->getPdo()->prepare($query);
            $this->bindValues($statement, $this->prepareBindings($bindings));
            $statement->execute();

            $this->recordsHaveBeenModified(
                ($count = $statement->rowCount()) > 0
            );

            return $count;
        });
    }

    public function unprepared($query)
    {
        return $this->run($query, [], function ($query) {
            if ($this->pretending()) {
                return true;
            }

            $this->notifyCacheOfWrite($query);

            $this->recordsHaveBeenModified(
                $change = $this->getPdo()->exec($query) !== false
            );

            return $change;
        });
    }

    public function disconnect()
    {
        $this->cachedPdo = null;
        parent::disconnect();
    }

    private function notifyCacheOfWrite($sql)
    {
        $writeTable = NativeCache::detectWrite($sql);
        if ($writeTable !== null) {
            $cache = NativeCache::getInstance();
            if ($writeTable === NativeCache::DDL_SENTINEL) {
                $cache->invalidateAll();
            } else {
                $cache->invalidateTable($writeTable);
            }
        }
    }
}
