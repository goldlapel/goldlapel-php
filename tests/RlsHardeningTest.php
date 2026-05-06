<?php

namespace GoldLapel\Tests;

use GoldLapel\ConnectionGucState;
use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the 2026-05-05 RLS-hardening pass — DISCARD parser fix
 * + set_config(...) function-form recognition.
 *
 * Covers:
 *   1. DISCARD parser fix (DISCARD ALL / PLANS / SEQUENCES / TEMP / TEMPORARY)
 *   2. set_config(name, value, is_local) function-form recognition
 *
 * Verify-on-checkout fallback (markDirty / applyVerifyResult) and
 * post-call verify on top-level SELECT <function>(...) ship in a
 * follow-up commit; tests for those are appended later.
 */
class RlsHardeningTest extends TestCase
{
    protected function setUp(): void
    {
        NativeCache::reset();
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
    }

    // ─── 1. DISCARD parser fix ──────────────────────────────────────────

    public function testParseDiscardAll(): void
    {
        $cmd = NativeCache::parseSetCommand('DISCARD ALL');
        $this->assertSame(
            ['type' => 'discard_all', 'name' => null, 'value' => null],
            $cmd,
        );
    }

    public function testParseDiscardAllCaseInsensitive(): void
    {
        $this->assertSame(
            ['type' => 'discard_all', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('discard all'),
        );
        $this->assertSame(
            ['type' => 'discard_all', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('Discard All'),
        );
    }

    public function testParseDiscardAllTolaratesTrailingSemicolon(): void
    {
        $this->assertSame(
            ['type' => 'discard_all', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('DISCARD ALL;'),
        );
    }

    public function testParseDiscardPlans(): void
    {
        $cmd = NativeCache::parseSetCommand('DISCARD PLANS');
        $this->assertSame(
            ['type' => 'discard_plans', 'name' => null, 'value' => null],
            $cmd,
        );
    }

    public function testParseDiscardSequences(): void
    {
        $this->assertSame(
            ['type' => 'discard_noop', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('DISCARD SEQUENCES'),
        );
    }

    public function testParseDiscardTemp(): void
    {
        $this->assertSame(
            ['type' => 'discard_noop', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('DISCARD TEMP'),
        );
        $this->assertSame(
            ['type' => 'discard_noop', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('DISCARD TEMPORARY'),
        );
    }

    public function testParseDiscardRejectsUnknownTarget(): void
    {
        $this->assertNull(NativeCache::parseSetCommand('DISCARD GARBAGE'));
        $this->assertNull(NativeCache::parseSetCommand('DISCARD ALL extra'));
        $this->assertNull(NativeCache::parseSetCommand('DISCARD'));
    }

    public function testDiscardAllClearsStateMap(): void
    {
        $state = new ConnectionGucState();
        $state->observeSql("SET app.user_id = '42'");
        $state->observeSql("SET role = 'tenant'");
        $this->assertNotSame('0', $state->stateHash());

        $changed = $state->observeSql('DISCARD ALL');
        $this->assertTrue($changed);
        $this->assertSame('0', $state->stateHash());
    }

    public function testDiscardAllOnEmptyStateIsNoOp(): void
    {
        $state = new ConnectionGucState();
        $changed = $state->observeSql('DISCARD ALL');
        $this->assertFalse($changed);
        $this->assertSame('0', $state->stateHash());
    }

    public function testDiscardPlansDoesNotClearState(): void
    {
        $state = new ConnectionGucState();
        $state->observeSql("SET app.user_id = '42'");
        $h = $state->stateHash();
        $changed = $state->observeSql('DISCARD PLANS');
        $this->assertFalse($changed);
        $this->assertSame($h, $state->stateHash());
    }

    public function testDiscardSequencesDoesNotClearState(): void
    {
        $state = new ConnectionGucState();
        $state->observeSql("SET app.user_id = '42'");
        $h = $state->stateHash();
        foreach (['DISCARD SEQUENCES', 'DISCARD TEMP', 'DISCARD TEMPORARY'] as $sql) {
            $changed = $state->observeSql($sql);
            $this->assertFalse($changed);
        }
        $this->assertSame($h, $state->stateHash());
    }

    // ─── 2. set_config() function-form ──────────────────────────────────

    public function testParseSetConfigBasic(): void
    {
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.user_id', '42', false)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.user_id', 'value' => '42'],
            $cmd,
        );
    }

    public function testParseSetConfigPgCatalogQualified(): void
    {
        $cmd = NativeCache::parseSetCommand("SELECT pg_catalog.set_config('app.user_id', '42', false)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.user_id', 'value' => '42'],
            $cmd,
        );
    }

    public function testParseSetConfigIsLocalTrue(): void
    {
        // is_local=true → set_config_local, ignored by state hash.
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.user_id', '42', true)");
        $this->assertSame(
            ['type' => 'set_config_local', 'name' => 'app.user_id', 'value' => '42'],
            $cmd,
        );
    }

    public function testParseSetConfigCaseInsensitive(): void
    {
        $cmd = NativeCache::parseSetCommand("select SET_CONFIG('app.user_id', '42', FALSE)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.user_id', 'value' => '42'],
            $cmd,
        );
    }

    public function testParseSetConfigTrailingSemicolon(): void
    {
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.user_id', '42', false);");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.user_id', 'value' => '42'],
            $cmd,
        );
    }

    public function testParseSetConfigQuotedBoolean(): void
    {
        // Supabase client libs sometimes emit `'true'` / `'false'`.
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.user_id', '42', 'false')");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.user_id', 'value' => '42'],
            $cmd,
        );
        $cmd2 = NativeCache::parseSetCommand("SELECT set_config('app.user_id', '42', 't')");
        $this->assertSame(
            ['type' => 'set_config_local', 'name' => 'app.user_id', 'value' => '42'],
            $cmd2,
        );
    }

    public function testParseSetConfigParenInQuotedValue(): void
    {
        // The outer-paren regex is greedy but the splitter is quote-
        // aware, so a literal `)` inside a string literal doesn't
        // confuse the parse.
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.note', 'foo)bar', false)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.note', 'value' => 'foo)bar'],
            $cmd,
        );
    }

    public function testParseSetConfigCommaInQuotedValue(): void
    {
        // Comma inside a string literal must NOT split args.
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.csv', 'a,b,c', false)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.csv', 'value' => 'a,b,c'],
            $cmd,
        );
    }

    public function testParseSetConfigDoubledQuoteInValue(): void
    {
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.note', 'it''s ok', false)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => 'app.note', 'value' => "it's ok"],
            $cmd,
        );
    }

    public function testParseSetConfigDoubledQuoteInName(): void
    {
        // Pathological but legal: doubled-quote in a name.
        $cmd = NativeCache::parseSetCommand("SELECT set_config('app.weird''name', 'v', false)");
        $this->assertSame(
            ['type' => 'set_config', 'name' => "app.weird'name", 'value' => 'v'],
            $cmd,
        );
    }

    public function testParseSetConfigRejectsNonLiteralName(): void
    {
        // Column reference / variable: we can't reduce to a constant,
        // post-call verify path picks it up instead.
        $this->assertNull(
            NativeCache::parseSetCommand("SELECT set_config(my_col, '42', false)"),
        );
    }

    public function testParseSetConfigRejectsWrongArgCount(): void
    {
        $this->assertNull(NativeCache::parseSetCommand("SELECT set_config('app.user_id', '42')"));
        $this->assertNull(NativeCache::parseSetCommand("SELECT set_config()"));
        $this->assertNull(NativeCache::parseSetCommand("SELECT set_config('a', 'b', 'c', 'd')"));
    }

    public function testSetConfigMutatesStateHashWhenNotLocal(): void
    {
        $state = new ConnectionGucState();
        $changed = $state->observeSql("SELECT set_config('app.user_id', '42', false)");
        $this->assertTrue($changed);
        $this->assertNotSame('0', $state->stateHash());

        // Match what a regular SET would produce.
        $other = new ConnectionGucState();
        $other->observeSql("SET app.user_id = '42'");
        $this->assertSame($other->stateHash(), $state->stateHash());
    }

    public function testSetConfigLocalDoesNotMutateState(): void
    {
        $state = new ConnectionGucState();
        $changed = $state->observeSql("SELECT set_config('app.user_id', '42', true)");
        $this->assertFalse($changed);
        $this->assertSame('0', $state->stateHash());
    }

    public function testSetConfigSafeNameIsIgnoredByState(): void
    {
        // set_config can target safe GUCs too — treat exactly like SET.
        $state = new ConnectionGucState();
        $changed = $state->observeSql("SELECT set_config('application_name', 'svc', false)");
        $this->assertFalse($changed);
        $this->assertSame('0', $state->stateHash());
    }
}
