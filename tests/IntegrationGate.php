<?php
declare(strict_types=1);

namespace GoldLapel\Tests;

use RuntimeException;

/**
 * Shared integration-test gating — standardized across all Gold Lapel wrappers.
 *
 * Convention:
 *   - GOLDLAPEL_INTEGRATION=1  — explicit opt-in gate ("yes, really run these")
 *   - GOLDLAPEL_TEST_UPSTREAM  — Postgres URL for the test upstream
 *
 * Both must be set. If GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM
 * is missing, the gate throws RuntimeException — this prevents a half-configured
 * CI from silently skipping integration tests and producing a false-green
 * unit-only run.
 *
 * If GOLDLAPEL_INTEGRATION is unset, integration tests skip silently.
 */
final class IntegrationGate
{
    /**
     * Returns the upstream Postgres URL if integration tests should run, or
     * null if they should skip. Throws RuntimeException if GOLDLAPEL_INTEGRATION=1
     * is set but GOLDLAPEL_TEST_UPSTREAM is missing (false-green prevention).
     */
    public static function upstream(): ?string
    {
        $integration = getenv('GOLDLAPEL_INTEGRATION') === '1';
        $upstream = getenv('GOLDLAPEL_TEST_UPSTREAM');
        $upstream = ($upstream === false || $upstream === '') ? null : $upstream;

        if ($integration && $upstream === null) {
            throw new RuntimeException(
                'GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is ' .
                'missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL ' .
                '(e.g. postgresql://postgres@localhost/postgres) or unset ' .
                'GOLDLAPEL_INTEGRATION to skip integration tests.'
            );
        }

        return $integration ? $upstream : null;
    }

    /**
     * Standardized skip reason for PHPUnit markTestSkipped().
     */
    public static function skipReason(): string
    {
        return 'set GOLDLAPEL_INTEGRATION=1 and GOLDLAPEL_TEST_UPSTREAM to run';
    }
}
