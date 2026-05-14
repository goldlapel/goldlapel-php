<?php

namespace GoldLapel\Tests;

use GoldLapel\AggressiveVerify;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for the simplified AggressiveVerify mode-resolution module.
 *
 * Wave 5 collapsed the previous smart-auto-enable machinery (pg_trigger
 * probe + per-(database, process) detection cache + license-payload
 * bit) into a flat three-mode decision: 'auto' / 'on' → bump,
 * 'off' → no bump (with a one-shot warning). The actual cache-key
 * isolation work is done by ConnectionGucState::bumpDmlSeq(); this
 * module just resolves the mode string and emits the opt-out warning.
 *
 * Wiring tests (does CachedPDO + Amp\CachedConnection actually call
 * bumpDmlSeq after a DML?) live in tests/AggressiveVerifyWiringTest.php
 * and tests/Amp/AggressiveVerifyTest.php.
 */
class AggressiveVerifyTest extends TestCase
{
    protected function setUp(): void
    {
        AggressiveVerify::clearCache();
    }

    protected function tearDown(): void
    {
        AggressiveVerify::clearCache();
    }

    public function testAutoModeBumps(): void
    {
        // 'auto' is the documented default — always bump.
        $this->assertTrue(AggressiveVerify::decide('auto', 'k'));
    }

    public function testOnModeBumps(): void
    {
        // 'on' is the explicit opt-in. Same effective behaviour as
        // 'auto'; kept as a distinct mode so a future smart-detect
        // pass can flip 'auto' off without breaking explicit 'on'.
        $this->assertTrue(AggressiveVerify::decide('on', 'k'));
    }

    public function testOffModeDoesNotBump(): void
    {
        $this->assertFalse(@AggressiveVerify::decide('off', 'k'));
    }

    public function testOffModeEmitsWarningOnce(): void
    {
        // Capture trigger_error output. Tested with the @-suppress
        // pattern: a deprecation/warning emitted under @ is swallowed
        // entirely (PHP doesn't enter the error handler) — we want the
        // warning to fire here, so use set_error_handler instead.
        $caught = [];
        set_error_handler(function ($errno, $msg) use (&$caught) {
            $caught[] = [$errno, $msg];
            return true;
        }, E_USER_WARNING);
        try {
            AggressiveVerify::decide('off', 'connection-1');
            $this->assertCount(1, $caught, 'off-mode should emit a warning');
            $this->assertSame(E_USER_WARNING, $caught[0][0]);
            $this->assertStringContainsString('aggressive_verify', $caught[0][1]);
            $this->assertStringContainsString('off', $caught[0][1]);

            // Second call with same cache key — warning is deduped.
            AggressiveVerify::decide('off', 'connection-1');
            $this->assertCount(1, $caught, 'duplicate off-mode call must not re-warn');

            // Different cache key → fires again. Distinct connections
            // each surface the warning once.
            AggressiveVerify::decide('off', 'connection-2');
            $this->assertCount(2, $caught, 'distinct connection identity should re-warn');
        } finally {
            restore_error_handler();
        }
    }

    public function testClearCacheResetsWarningTracker(): void
    {
        $caught = 0;
        set_error_handler(function () use (&$caught) {
            $caught++;
            return true;
        }, E_USER_WARNING);
        try {
            AggressiveVerify::decide('off', 'k');
            $this->assertSame(1, $caught);
            // Dedup on the same key.
            AggressiveVerify::decide('off', 'k');
            $this->assertSame(1, $caught);
            // After clearCache(), the warning re-fires.
            AggressiveVerify::clearCache();
            AggressiveVerify::decide('off', 'k');
            $this->assertSame(2, $caught);
        } finally {
            restore_error_handler();
        }
    }

    public function testUnrecognisedModeIsTreatedAsAuto(): void
    {
        // Lenient parsing — anything other than 'off' falls through to
        // bump. Avoids surprising users who type 'true' / 'maybe' /
        // mis-spell; correctness-first default.
        $this->assertTrue(AggressiveVerify::decide('garbage', 'k'));
        $this->assertTrue(AggressiveVerify::decide('', 'k'));
    }

    public function testModeIsCaseInsensitive(): void
    {
        $this->assertTrue(AggressiveVerify::decide('ON', 'k'));
        $this->assertTrue(AggressiveVerify::decide('On', 'k2'));
        $this->assertTrue(AggressiveVerify::decide('AUTO', 'k3'));
        $this->assertFalse(@AggressiveVerify::decide('OFF', 'k4'));
        $this->assertFalse(@AggressiveVerify::decide('Off', 'k5'));
    }
}
