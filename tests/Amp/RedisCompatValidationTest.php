<?php

namespace GoldLapel\Amp\Tests;

use Amp\Postgres\PostgresExecutor;
use GoldLapel\Amp\Utils;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;

/**
 * Regression: Amp `publish()` must reject injection-shaped channel
 * identifiers for parity with the sync path and the other 6 language
 * wrappers.
 *
 * Historically, Amp `publish()` was the only place across wrappers
 * that skipped `validateIdentifier()` on the channel arg. The call is
 * parameterised via `pg_notify($1, $2)` so it isn't an exploitable
 * injection surface, but we validate for defence-in-depth + cross-
 * wrapper consistency (see cross-wrapper-bug-sweep.md, Design item 2).
 */
#[AllowMockObjectsWithoutExpectations]
class RedisCompatValidationTest extends TestCase
{
    private const BAD = 'foo; DROP TABLE users--';

    public function testPublishRejectsBadChannel(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::publish($this->createMock(PostgresExecutor::class), self::BAD, 'payload');
    }
}
