<?php

namespace GoldLapel\Amp\Tests;

use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresExecutor;
use Amp\Postgres\PostgresResult;
use GoldLapel\Amp\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests that the async Amp trigger-creation paths emit the atomic
 * `CREATE OR REPLACE TRIGGER` form (PG14+) rather than the racy
 * `DROP TRIGGER IF EXISTS` + `CREATE TRIGGER` pair.
 *
 * Parity with the sync `DocTest` cases of the same name, and with the
 * equivalent Go / Python / JS / Ruby / Java / .NET regression tests.
 *
 * See goldlapel-go@574b551 for the originating bug fix — concurrent
 * doc_watch calls could have the DROP complete before the CREATE of a
 * peer fiber, leaving the table without any NOTIFY trigger for a window.
 */
#[AllowMockObjectsWithoutExpectations]
class DocTriggerAtomicTest extends TestCase
{
    /**
     * Build a mock PostgresConnection whose query() records every SQL
     * string into $captured and returns a no-op PostgresResult mock.
     */
    private function makeConn(array &$captured): PostgresConnection
    {
        $result = $this->createMock(PostgresResult::class);
        $conn = $this->createMock(PostgresConnection::class);
        $conn->method('query')->willReturnCallback(function ($sql) use (&$captured, $result) {
            $captured[] = $sql;
            return $result;
        });
        return $conn;
    }

    /**
     * Build a mock bare PostgresExecutor for methods that don't need
     * LISTEN/NOTIFY (TTL, capped).
     */
    private function makeExecutor(array &$captured): PostgresExecutor
    {
        $result = $this->createMock(PostgresResult::class);
        $exec = $this->createMock(PostgresExecutor::class);
        $exec->method('query')->willReturnCallback(function ($sql) use (&$captured, $result) {
            $captured[] = $sql;
            return $result;
        });
        return $exec;
    }

    private function fakePatterns(string $collection): array
    {
        return [
            'tables' => ['main' => $collection],
            'query_patterns' => [],
        ];
    }

    public function testDocWatchEmitsAtomicCreateOrReplaceTrigger(): void
    {
        $captured = [];
        $conn = $this->makeConn($captured);

        // No callback — docWatch should return after setting up the
        // function + trigger without entering the listen loop.
        Utils::docWatch($conn, 'orders', patterns: $this->fakePatterns('orders'));

        $joined = implode("\n---\n", $captured);

        // Atomic CREATE OR REPLACE TRIGGER (PG14+) — matches the Go wrapper.
        // Avoids the race where a DROP + CREATE pair could have two
        // concurrent docWatch calls briefly leave no trigger.
        $this->assertStringContainsString('CREATE OR REPLACE TRIGGER orders_notify_trg', $joined);
        $this->assertStringContainsString('AFTER INSERT OR UPDATE OR DELETE ON orders', $joined);
        $this->assertStringContainsString('EXECUTE FUNCTION orders_notify_fn()', $joined);

        // Guard against the racy DROP + CREATE pair regressing.
        foreach ($captured as $sql) {
            $this->assertStringNotContainsString('DROP TRIGGER IF EXISTS orders_notify_trg', $sql);
        }
    }

    public function testDocCreateTtlIndexEmitsAtomicCreateOrReplaceTrigger(): void
    {
        $captured = [];
        $exec = $this->makeExecutor($captured);

        Utils::docCreateTtlIndex($exec, 'sessions', 3600, patterns: $this->fakePatterns('sessions'));

        $joined = implode("\n---\n", $captured);

        $this->assertStringContainsString('CREATE OR REPLACE TRIGGER sessions_ttl_trg', $joined);
        $this->assertStringContainsString('BEFORE INSERT ON sessions', $joined);
        $this->assertStringContainsString('EXECUTE FUNCTION sessions_ttl_fn()', $joined);

        foreach ($captured as $sql) {
            $this->assertStringNotContainsString('DROP TRIGGER IF EXISTS sessions_ttl_trg', $sql);
        }
    }

    public function testDocCreateCappedEmitsAtomicCreateOrReplaceTrigger(): void
    {
        $captured = [];
        $exec = $this->makeExecutor($captured);

        Utils::docCreateCapped($exec, 'logs', 1000, patterns: $this->fakePatterns('logs'));

        $joined = implode("\n---\n", $captured);

        $this->assertStringContainsString('CREATE OR REPLACE TRIGGER logs_cap_trg', $joined);
        $this->assertStringContainsString('AFTER INSERT ON logs', $joined);
        $this->assertStringContainsString('EXECUTE FUNCTION logs_cap_fn()', $joined);

        foreach ($captured as $sql) {
            $this->assertStringNotContainsString('DROP TRIGGER IF EXISTS logs_cap_trg', $sql);
        }
    }
}
