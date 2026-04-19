<?php

namespace GoldLapel\Amp;

use Amp\Postgres\PostgresResult;

/**
 * An in-memory PostgresResult replaying buffered rows. Used by
 * CachedConnection when a read hits the L1 cache — the original
 * PostgresResult has already been drained, so we reconstruct the
 * iteration shape from the cached rows.
 *
 * Only implements the read-side of PostgresResult (iteration +
 * fetchRow + getRowCount). getColumnCount returns the width of the
 * first row. getNextResult returns null (no further result sets).
 */
class CachedResult implements PostgresResult, \IteratorAggregate
{
    private int $position = 0;

    /** @param list<array<string, mixed>> $rows */
    public function __construct(private array $rows)
    {
    }

    public function getIterator(): \Iterator
    {
        foreach ($this->rows as $i => $row) {
            yield $i => $row;
        }
    }

    public function fetchRow(): ?array
    {
        if ($this->position >= count($this->rows)) {
            return null;
        }
        return $this->rows[$this->position++];
    }

    public function getNextResult(): ?PostgresResult
    {
        return null;
    }

    public function getRowCount(): ?int
    {
        return count($this->rows);
    }

    public function getColumnCount(): ?int
    {
        if (empty($this->rows)) {
            return 0;
        }
        return count($this->rows[0]);
    }
}
