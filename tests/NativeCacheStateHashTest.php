<?php

namespace GoldLapel\Tests;

use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the wrapper-side L1 unsafe-GUC state-hash (Option Y).
 *
 * Mirrors the proxy commit `3e02359` ("proxy: Wave 2.5 — Option Y GUC-RLS
 * cache safety"). The wrapper cache must fold a per-connection
 * unsafe-GUC fingerprint into the cache key so that a `SET app.user_id =
 * '7'` followed by the same SELECT can't hit a slot populated for user
 * `42`. Same idea, smaller surface — the wrapper only handles the
 * single-connection case.
 *
 * Coverage:
 *   1. is_unsafe_guc classification (short list, namespaced, case)
 *   2. parse_set_command shapes (= / TO / SESSION / LOCAL / glued = /
 *      quoted name / quoted value / case insensitivity / RESET / RESET ALL)
 *   3. split_statements respects single + double quoted strings + doubled
 *      quote escapes
 *   4. observeSql() applies state, mutates hash, ignores SET LOCAL +
 *      safe GUCs, handles multi-statement bodies
 *   5. State-hash invariants — empty state is "0", insertion-order
 *      independent, reset round-trip restores the old hash
 *   6. End-to-end cache: same SQL with different unsafe GUC state must
 *      not share a cache slot
 */
class NativeCacheStateHashTest extends TestCase
{
    protected function setUp(): void
    {
        NativeCache::reset();
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
    }

    private function makeCache(): NativeCache
    {
        $cache = new NativeCache();
        $cache->setConnected(true);
        return $cache;
    }

    // ─── is_unsafe_guc ───────────────────────────────────────────────────

    public function testUnsafeShortListMembers(): void
    {
        $this->assertTrue(NativeCache::isUnsafeGuc('search_path'));
        $this->assertTrue(NativeCache::isUnsafeGuc('role'));
        $this->assertTrue(NativeCache::isUnsafeGuc('session_authorization'));
        $this->assertTrue(NativeCache::isUnsafeGuc('default_transaction_isolation'));
        $this->assertTrue(NativeCache::isUnsafeGuc('default_transaction_read_only'));
        $this->assertTrue(NativeCache::isUnsafeGuc('transaction_isolation'));
        $this->assertTrue(NativeCache::isUnsafeGuc('row_security'));
    }

    public function testUnsafeClassificationCaseInsensitive(): void
    {
        $this->assertTrue(NativeCache::isUnsafeGuc('ROLE'));
        $this->assertTrue(NativeCache::isUnsafeGuc('Search_Path'));
        $this->assertTrue(NativeCache::isUnsafeGuc('SEARCH_PATH'));
    }

    public function testNamespacedGucsAreUnsafe(): void
    {
        $this->assertTrue(NativeCache::isUnsafeGuc('app.user_id'));
        $this->assertTrue(NativeCache::isUnsafeGuc('myapp.tenant'));
        $this->assertTrue(NativeCache::isUnsafeGuc('rls.account'));
        // Even unknown / arbitrarily nested namespaces.
        $this->assertTrue(NativeCache::isUnsafeGuc('a.b.c'));
        $this->assertTrue(NativeCache::isUnsafeGuc('APP.USER'));
    }

    public function testSafeGucsAreSafe(): void
    {
        $this->assertFalse(NativeCache::isUnsafeGuc('timezone'));
        $this->assertFalse(NativeCache::isUnsafeGuc('application_name'));
        $this->assertFalse(NativeCache::isUnsafeGuc('statement_timeout'));
        $this->assertFalse(NativeCache::isUnsafeGuc('work_mem'));
        $this->assertFalse(NativeCache::isUnsafeGuc('client_encoding'));
        $this->assertFalse(NativeCache::isUnsafeGuc('DateStyle'));
    }

    // ─── parse_set_command ───────────────────────────────────────────────

    public function testParseSetEqQuoted(): void
    {
        $cmd = NativeCache::parseSetCommand("SET foo = 'bar'");
        $this->assertSame(['type' => 'set', 'name' => 'foo', 'value' => 'bar'], $cmd);
    }

    public function testParseSetToQuoted(): void
    {
        $cmd = NativeCache::parseSetCommand("SET foo TO 'bar'");
        $this->assertSame(['type' => 'set', 'name' => 'foo', 'value' => 'bar'], $cmd);
    }

    public function testParseSetUnquoted(): void
    {
        $cmd = NativeCache::parseSetCommand('SET foo = 42');
        $this->assertSame(['type' => 'set', 'name' => 'foo', 'value' => '42'], $cmd);
    }

    public function testParseSetSessionModifier(): void
    {
        $cmd = NativeCache::parseSetCommand("SET SESSION foo = 'bar'");
        $this->assertSame(['type' => 'set', 'name' => 'foo', 'value' => 'bar'], $cmd);
    }

    public function testParseSetLocalModifier(): void
    {
        $cmd = NativeCache::parseSetCommand("SET LOCAL foo = 'bar'");
        $this->assertSame(['type' => 'set_local', 'name' => 'foo', 'value' => 'bar'], $cmd);
    }

    public function testParseResetNamed(): void
    {
        $cmd = NativeCache::parseSetCommand('RESET foo');
        $this->assertSame(['type' => 'reset', 'name' => 'foo', 'value' => null], $cmd);
    }

    public function testParseResetAll(): void
    {
        $cmd = NativeCache::parseSetCommand('RESET ALL');
        $this->assertSame(['type' => 'reset_all', 'name' => null, 'value' => null], $cmd);
    }

    public function testParseGluedEqualsSign(): void
    {
        // `SET app.user='42'` — = glued onto the name token.
        $cmd = NativeCache::parseSetCommand("SET app.user='42'");
        $this->assertSame(['type' => 'set', 'name' => 'app.user', 'value' => '42'], $cmd);
    }

    public function testParseQuotedGucName(): void
    {
        // PG accepts `"app.user_id"` as the GUC name; we lowercase + strip quotes.
        $cmd = NativeCache::parseSetCommand('SET "App.User_ID" = 42');
        $this->assertSame(['type' => 'set', 'name' => 'app.user_id', 'value' => '42'], $cmd);
    }

    public function testParseCaseInsensitiveKeywords(): void
    {
        $this->assertSame(
            ['type' => 'set', 'name' => 'foo', 'value' => 'bar'],
            NativeCache::parseSetCommand("set foo = 'bar'"),
        );
        $this->assertSame(
            ['type' => 'set_local', 'name' => 'foo', 'value' => 'bar'],
            NativeCache::parseSetCommand("Set Local foo To 'bar'"),
        );
        $this->assertSame(
            ['type' => 'reset_all', 'name' => null, 'value' => null],
            NativeCache::parseSetCommand('reset all'),
        );
    }

    public function testParseLowercasesGucName(): void
    {
        $this->assertSame(
            ['type' => 'set', 'name' => 'app.user_id', 'value' => '42'],
            NativeCache::parseSetCommand("SET App.User_ID = '42'"),
        );
    }

    public function testParseTolaratesTrailingSemicolon(): void
    {
        $this->assertSame(
            ['type' => 'set', 'name' => 'foo', 'value' => 'bar'],
            NativeCache::parseSetCommand("SET foo = 'bar';"),
        );
        $this->assertSame(
            ['type' => 'reset', 'name' => 'foo', 'value' => null],
            NativeCache::parseSetCommand('RESET foo ;'),
        );
    }

    public function testParseRejectsNonSetReset(): void
    {
        $this->assertNull(NativeCache::parseSetCommand('SELECT 1'));
        $this->assertNull(NativeCache::parseSetCommand('UPDATE x SET y = 1'));
        $this->assertNull(NativeCache::parseSetCommand(''));
        $this->assertNull(NativeCache::parseSetCommand('   '));
    }

    public function testParseRejectsSetTimeZone(): void
    {
        // SET TIME ZONE is harmless (timezone is a safe GUC) and the
        // two-word GUC name doesn't fit our pattern. Returning null is
        // correct because it doesn't affect cache safety.
        $this->assertNull(NativeCache::parseSetCommand("SET TIME ZONE 'UTC'"));
    }

    public function testParseRejectsResetWithExtraTokens(): void
    {
        // `RESET name junk` — extra tokens after the name make this not
        // a clean `RESET name` shape. Reject.
        $this->assertNull(NativeCache::parseSetCommand('RESET foo bar'));
    }

    // ─── split_statements ────────────────────────────────────────────────

    public function testSplitSingleStatement(): void
    {
        $this->assertSame(['SELECT 1'], NativeCache::splitStatements('SELECT 1'));
    }

    public function testSplitTrailingSemicolon(): void
    {
        $this->assertSame(['SELECT 1'], NativeCache::splitStatements('SELECT 1;'));
    }

    public function testSplitMultipleStatements(): void
    {
        $this->assertSame(
            ['SET foo = 1', 'SELECT 1'],
            NativeCache::splitStatements('SET foo = 1; SELECT 1'),
        );
    }

    public function testSplitRespectsSingleQuotedStrings(): void
    {
        // `;` inside a quoted string MUST NOT split.
        $this->assertSame(
            ["SET app.note = 'a;b'", 'SELECT 1'],
            NativeCache::splitStatements("SET app.note = 'a;b'; SELECT 1"),
        );
    }

    public function testSplitRespectsDoubleQuotedStrings(): void
    {
        $this->assertSame(
            ['SET "x;y" = 1', 'SELECT 1'],
            NativeCache::splitStatements('SET "x;y" = 1; SELECT 1'),
        );
    }

    public function testSplitHandlesDoubledQuoteEscape(): void
    {
        // `''` is PG's doubled-quote escape — stays inside the literal.
        $this->assertSame(
            ["SET app.note = 'it''s; ok'", 'SELECT 1'],
            NativeCache::splitStatements("SET app.note = 'it''s; ok'; SELECT 1"),
        );
    }

    public function testSplitDropsEmptySegments(): void
    {
        $this->assertSame(['SELECT 1'], NativeCache::splitStatements(';;SELECT 1;;'));
    }

    // ─── observeSql + state hash ────────────────────────────────────────

    public function testEmptyStateHashIsZero(): void
    {
        $cache = $this->makeCache();
        $this->assertSame('0', $cache->getStateHash());
    }

    public function testObserveUnsafeSetMutatesHash(): void
    {
        $cache = $this->makeCache();
        $changed = $cache->observeSql("SET app.user_id = '42'");
        $this->assertTrue($changed);
        $this->assertNotSame('0', $cache->getStateHash());
        // Populated-state hashes are 16-char lowercase hex (xxh64 width).
        $this->assertSame(16, strlen($cache->getStateHash()));
        $this->assertMatchesRegularExpression('/^[0-9a-f]{16}$/', $cache->getStateHash());
    }

    public function testObserveSafeSetDoesNotMutateHash(): void
    {
        $cache = $this->makeCache();
        $changed = $cache->observeSql("SET application_name = 'myapp'");
        $this->assertFalse($changed);
        $this->assertSame('0', $cache->getStateHash());
    }

    public function testObserveSetLocalIsNoOpForState(): void
    {
        // SET LOCAL is parsed but ignored — cache is gated on
        // transaction-idle so SET LOCAL never influences a cacheable
        // response.
        $cache = $this->makeCache();
        $changed = $cache->observeSql("SET LOCAL app.user_id = '42'");
        $this->assertFalse($changed);
        $this->assertSame('0', $cache->getStateHash());
    }

    public function testObserveResetClearsSpecificGuc(): void
    {
        $cache = $this->makeCache();
        $cache->observeSql("SET app.user_id = '42'");
        $with = $cache->getStateHash();
        $cache->observeSql('RESET app.user_id');
        $this->assertSame('0', $cache->getStateHash());
        $this->assertNotSame($with, $cache->getStateHash());
    }

    public function testObserveResetAllClearsAllUnsafeGucs(): void
    {
        $cache = $this->makeCache();
        $cache->observeSql("SET app.user_id = '42'");
        $cache->observeSql("SET role = 'tenant'");
        $this->assertNotSame('0', $cache->getStateHash());
        $cache->observeSql('RESET ALL');
        $this->assertSame('0', $cache->getStateHash());
    }

    public function testObserveDifferentValuesYieldDifferentHashes(): void
    {
        $cache1 = $this->makeCache();
        $cache1->observeSql("SET app.user_id = '42'");
        $cache2 = $this->makeCache();
        $cache2->observeSql("SET app.user_id = '43'");
        $this->assertNotSame($cache1->getStateHash(), $cache2->getStateHash());
    }

    public function testObserveInsertionOrderIndependent(): void
    {
        // BTreeMap parity: hash is stable across insertion orders.
        $cache1 = $this->makeCache();
        $cache1->observeSql("SET app.a = '1'");
        $cache1->observeSql("SET app.b = '2'");
        $cache2 = $this->makeCache();
        $cache2->observeSql("SET app.b = '2'");
        $cache2->observeSql("SET app.a = '1'");
        $this->assertSame($cache1->getStateHash(), $cache2->getStateHash());
    }

    public function testObserveResetRoundTripRestoresHash(): void
    {
        // Starting from empty, set then reset should restore the empty
        // hash exactly. Set+reset is the canonical "audit your handler
        // didn't leak state" round-trip.
        $cache = $this->makeCache();
        $before = $cache->getStateHash();
        $cache->observeSql("SET app.user_id = '42'");
        $cache->observeSql('RESET app.user_id');
        $this->assertSame($before, $cache->getStateHash());
    }

    public function testObserveMultiStatementBodyAppliesAllSets(): void
    {
        $cache = $this->makeCache();
        $changed = $cache->observeSql("SET app.user_id = '42'; SET role = 'tenant'; SELECT 1");
        $this->assertTrue($changed);
        // The hash should match observing the same SETs individually.
        $other = $this->makeCache();
        $other->observeSql("SET app.user_id = '42'");
        $other->observeSql("SET role = 'tenant'");
        $this->assertSame($other->getStateHash(), $cache->getStateHash());
    }

    public function testObserveReSetSameValueIsIdempotent(): void
    {
        $cache = $this->makeCache();
        $cache->observeSql("SET app.user_id = '42'");
        $h = $cache->getStateHash();
        $cache->observeSql("SET app.user_id = '42'");
        $this->assertSame($h, $cache->getStateHash());
    }

    // ─── End-to-end cache: state hash gates cache-key sharing ───────────

    public function testCacheKeyDifferentForDifferentStateHashes(): void
    {
        // Two cache keys with different state hashes for the same SQL must
        // be distinct strings — the state hash is folded into the key.
        $sql = 'SELECT * FROM accounts';
        $k0 = NativeCache::makeKey($sql, null, '0');
        $k1 = NativeCache::makeKey($sql, null, 'deadbeefcafe1234');
        $this->assertNotSame($k0, $k1);
    }

    public function testCacheHitOnlyWhenStateHashMatches(): void
    {
        // Populate cache slot under user A's state, then switch to user B
        // and try to read the same SQL — must miss.
        $cache = $this->makeCache();
        $sql = 'SELECT * FROM accounts';

        $cache->observeSql("SET app.user_id = 'A'");
        $cache->put($sql, null, [['id' => 'A1']], ['id']);
        $hitA = $cache->get($sql, null);
        $this->assertSame([['id' => 'A1']], $hitA['rows']);

        // Switch to user B — state hash changes — same SQL must miss.
        $missesBefore = $cache->statsMisses;
        $cache->observeSql("SET app.user_id = 'B'");
        $hitB = $cache->get($sql, null);
        $this->assertNull($hitB);
        $this->assertSame($missesBefore + 1, $cache->statsMisses);

        // And user B can populate their own slot without colliding.
        $cache->put($sql, null, [['id' => 'B1']], ['id']);
        $this->assertSame([['id' => 'B1']], $cache->get($sql, null)['rows']);

        // Swap back — user A still gets their own slot.
        $cache->observeSql("SET app.user_id = 'A'");
        $this->assertSame([['id' => 'A1']], $cache->get($sql, null)['rows']);
    }

    public function testCacheSlotSharedAcrossSetLocalSinceLocalIsIgnored(): void
    {
        // SET LOCAL is intentionally ignored for state-hash purposes — the
        // proxy gates cache participation on transaction-idle anyway. Two
        // get()s sandwiched around a SET LOCAL on the same SQL must hit
        // the same slot.
        $cache = $this->makeCache();
        $sql = 'SELECT 1';
        $cache->put($sql, null, [['n' => 1]], ['n']);

        $hitBefore = $cache->get($sql, null);
        $cache->observeSql("SET LOCAL app.tenant = 'x'");
        $hitAfter = $cache->get($sql, null);

        $this->assertSame($hitBefore['rows'], $hitAfter['rows']);
    }

    public function testCacheSlotSharedAcrossSafeGucMutation(): void
    {
        // Safe GUCs (timezone, application_name, statement_timeout, …)
        // never enter the state map.
        $cache = $this->makeCache();
        $sql = 'SELECT 1';
        $cache->put($sql, null, [['n' => 1]], ['n']);

        $cache->observeSql("SET application_name = 'svc-a'");
        $hitA = $cache->get($sql, null);
        $cache->observeSql("SET application_name = 'svc-b'");
        $hitB = $cache->get($sql, null);

        $this->assertSame($hitA['rows'], $hitB['rows']);
    }
}
