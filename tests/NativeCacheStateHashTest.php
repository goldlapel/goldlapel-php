<?php

namespace GoldLapel\Tests;

use GoldLapel\ConnectionGucState;
use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the wrapper-side L1 unsafe-GUC state-hash (Option Y).
 *
 * Mirrors the proxy commit `3e02359` ("proxy: Wave 2.5 — Option Y GUC-RLS
 * cache safety"). The wrapper cache must fold a per-connection
 * unsafe-GUC fingerprint into the cache key so that a `SET app.user_id =
 * '7'` followed by the same SELECT can't hit a slot populated for user
 * `42`. State lives on ConnectionGucState — one tracker per CachedPDO /
 * CachedConnection — so concurrent connections can never share GUC
 * state through a singleton (matches goldlapel-python /
 * goldlapel-ruby / goldlapel-js).
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
 *   7. Cross-connection isolation — two ConnectionGucState instances
 *      pointing at the same shared NativeCache never collide.
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

    private function makeState(): ConnectionGucState
    {
        return new ConnectionGucState();
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
        // application_name / statement_timeout / work_mem / client_encoding
        // never affect query result content — pure operational tuning
        // knobs. Safe to share cache slots across mutations.
        $this->assertFalse(NativeCache::isUnsafeGuc('application_name'));
        $this->assertFalse(NativeCache::isUnsafeGuc('statement_timeout'));
        $this->assertFalse(NativeCache::isUnsafeGuc('work_mem'));
        $this->assertFalse(NativeCache::isUnsafeGuc('client_encoding'));
    }

    public function testFormattingGucsAreUnsafe(): void
    {
        // 2026-05-05 expansion: locale + textual-formatting GUCs that can
        // change query output shape (datestyle / intervalstyle change
        // timestamp text rep, timezone shifts wall-clock conversions,
        // bytea_output changes bytea text encoding, lc_* affect locale-
        // sensitive collation / monetary / numeric formatting). All
        // case-insensitive.
        $this->assertTrue(NativeCache::isUnsafeGuc('DateStyle'));
        $this->assertTrue(NativeCache::isUnsafeGuc('datestyle'));
        $this->assertTrue(NativeCache::isUnsafeGuc('IntervalStyle'));
        $this->assertTrue(NativeCache::isUnsafeGuc('TimeZone'));
        $this->assertTrue(NativeCache::isUnsafeGuc('timezone'));
        $this->assertTrue(NativeCache::isUnsafeGuc('bytea_output'));
        $this->assertTrue(NativeCache::isUnsafeGuc('Bytea_Output'));
        $this->assertTrue(NativeCache::isUnsafeGuc('lc_messages'));
        $this->assertTrue(NativeCache::isUnsafeGuc('lc_monetary'));
        $this->assertTrue(NativeCache::isUnsafeGuc('lc_numeric'));
        $this->assertTrue(NativeCache::isUnsafeGuc('lc_time'));
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
        // `SET TIME ZONE 'UTC'` is the unusual two-word GUC name shape
        // PG accepts as an alias for `SET timezone = 'UTC'`. We don't
        // bother decomposing the multi-token name in the parser — the
        // far more common `SET timezone = 'UTC'` shape is already
        // recognised by the regular SET branch. Worst case for the
        // two-word form: a TIME ZONE change isn't reflected in the
        // wrapper's state hash; the proxy still has its own fingerprint
        // that catches it on the wire. Filed as a known limitation;
        // returning null here is intentional, not a leak.
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

    // ─── observeSql + state hash (ConnectionGucState) ───────────────────

    public function testEmptyStateHashIsZero(): void
    {
        $state = $this->makeState();
        $this->assertSame('0', $state->stateHash());
    }

    public function testObserveUnsafeSetMutatesHash(): void
    {
        $state = $this->makeState();
        $changed = $state->observeSql("SET app.user_id = '42'");
        $this->assertTrue($changed);
        $this->assertNotSame('0', $state->stateHash());
        // Populated-state hashes are 16-char lowercase hex (xxh64 width).
        $this->assertSame(16, strlen($state->stateHash()));
        $this->assertMatchesRegularExpression('/^[0-9a-f]{16}$/', $state->stateHash());
    }

    public function testObserveSafeSetDoesNotMutateHash(): void
    {
        $state = $this->makeState();
        $changed = $state->observeSql("SET application_name = 'myapp'");
        $this->assertFalse($changed);
        $this->assertSame('0', $state->stateHash());
    }

    public function testObserveSetLocalIsNoOpForState(): void
    {
        // SET LOCAL is parsed but ignored — cache is gated on
        // transaction-idle so SET LOCAL never influences a cacheable
        // response.
        $state = $this->makeState();
        $changed = $state->observeSql("SET LOCAL app.user_id = '42'");
        $this->assertFalse($changed);
        $this->assertSame('0', $state->stateHash());
    }

    public function testObserveResetClearsSpecificGuc(): void
    {
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $with = $state->stateHash();
        $state->observeSql('RESET app.user_id');
        $this->assertSame('0', $state->stateHash());
        $this->assertNotSame($with, $state->stateHash());
    }

    public function testObserveResetAllClearsAllUnsafeGucs(): void
    {
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $state->observeSql("SET role = 'tenant'");
        $this->assertNotSame('0', $state->stateHash());
        $state->observeSql('RESET ALL');
        $this->assertSame('0', $state->stateHash());
    }

    public function testObserveDifferentValuesYieldDifferentHashes(): void
    {
        $a = $this->makeState();
        $a->observeSql("SET app.user_id = '42'");
        $b = $this->makeState();
        $b->observeSql("SET app.user_id = '43'");
        $this->assertNotSame($a->stateHash(), $b->stateHash());
    }

    public function testObserveInsertionOrderIndependent(): void
    {
        // BTreeMap parity: hash is stable across insertion orders.
        $a = $this->makeState();
        $a->observeSql("SET app.a = '1'");
        $a->observeSql("SET app.b = '2'");
        $b = $this->makeState();
        $b->observeSql("SET app.b = '2'");
        $b->observeSql("SET app.a = '1'");
        $this->assertSame($a->stateHash(), $b->stateHash());
    }

    public function testObserveResetRoundTripRestoresHash(): void
    {
        // Starting from empty, set then reset should restore the empty
        // hash exactly. Set+reset is the canonical "audit your handler
        // didn't leak state" round-trip.
        $state = $this->makeState();
        $before = $state->stateHash();
        $state->observeSql("SET app.user_id = '42'");
        $state->observeSql('RESET app.user_id');
        $this->assertSame($before, $state->stateHash());
    }

    public function testObserveMultiStatementBodyAppliesAllSets(): void
    {
        $state = $this->makeState();
        $changed = $state->observeSql("SET app.user_id = '42'; SET role = 'tenant'; SELECT 1");
        $this->assertTrue($changed);
        // The hash should match observing the same SETs individually.
        $other = $this->makeState();
        $other->observeSql("SET app.user_id = '42'");
        $other->observeSql("SET role = 'tenant'");
        $this->assertSame($other->stateHash(), $state->stateHash());
    }

    public function testObserveReSetSameValueIsIdempotent(): void
    {
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $h = $state->stateHash();
        $state->observeSql("SET app.user_id = '42'");
        $this->assertSame($h, $state->stateHash());
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
        $state = $this->makeState();
        $sql = 'SELECT * FROM accounts';

        $state->observeSql("SET app.user_id = 'A'");
        $cache->put($sql, null, [['id' => 'A1']], ['id'], $state->stateHash());
        $hitA = $cache->get($sql, null, $state->stateHash());
        $this->assertSame([['id' => 'A1']], $hitA['rows']);

        // Switch to user B — state hash changes — same SQL must miss.
        $missesBefore = $cache->statsMisses;
        $state->observeSql("SET app.user_id = 'B'");
        $hitB = $cache->get($sql, null, $state->stateHash());
        $this->assertNull($hitB);
        $this->assertSame($missesBefore + 1, $cache->statsMisses);

        // And user B can populate their own slot without colliding.
        $cache->put($sql, null, [['id' => 'B1']], ['id'], $state->stateHash());
        $this->assertSame([['id' => 'B1']], $cache->get($sql, null, $state->stateHash())['rows']);

        // Swap back — user A still gets their own slot.
        $state->observeSql("SET app.user_id = 'A'");
        $this->assertSame([['id' => 'A1']], $cache->get($sql, null, $state->stateHash())['rows']);
    }

    public function testCacheSlotSharedAcrossSetLocalSinceLocalIsIgnored(): void
    {
        // SET LOCAL is intentionally ignored for state-hash purposes — the
        // proxy gates cache participation on transaction-idle anyway. Two
        // get()s sandwiched around a SET LOCAL on the same SQL must hit
        // the same slot.
        $cache = $this->makeCache();
        $state = $this->makeState();
        $sql = 'SELECT 1';
        $cache->put($sql, null, [['n' => 1]], ['n'], $state->stateHash());

        $hitBefore = $cache->get($sql, null, $state->stateHash());
        $state->observeSql("SET LOCAL app.tenant = 'x'");
        $hitAfter = $cache->get($sql, null, $state->stateHash());

        $this->assertSame($hitBefore['rows'], $hitAfter['rows']);
    }

    public function testCacheSlotSharedAcrossSafeGucMutation(): void
    {
        // Safe GUCs (application_name, statement_timeout, work_mem, …)
        // never enter the state map.
        $cache = $this->makeCache();
        $state = $this->makeState();
        $sql = 'SELECT 1';
        $cache->put($sql, null, [['n' => 1]], ['n'], $state->stateHash());

        $state->observeSql("SET application_name = 'svc-a'");
        $hitA = $cache->get($sql, null, $state->stateHash());
        $state->observeSql("SET application_name = 'svc-b'");
        $hitB = $cache->get($sql, null, $state->stateHash());

        $this->assertSame($hitA['rows'], $hitB['rows']);
    }

    // ─── Cross-connection isolation (regression — was the original bug) ─

    public function testTwoConnectionsDoNotShareGucState(): void
    {
        // Two ConnectionGucState instances pointing at the same shared
        // NativeCache must never see each other's GUC writes. This is the
        // canonical regression: putting state on the singleton cache (as
        // the wrapper originally did) lets connection A's `SET
        // app.user_id = 'A'` poison connection B's reads of the same SQL.
        $cache = $this->makeCache();
        $stateA = $this->makeState();
        $stateB = $this->makeState();

        $sql = 'SELECT * FROM accounts';

        // Connection A: SET, then put.
        $stateA->observeSql("SET app.user_id = 'A'");
        $cache->put($sql, null, [['id' => 'A1']], ['id'], $stateA->stateHash());

        // Connection B has issued no SET — its state hash is still '0',
        // distinct from A's. It must miss.
        $this->assertSame('0', $stateB->stateHash());
        $hitB = $cache->get($sql, null, $stateB->stateHash());
        $this->assertNull(
            $hitB,
            'connection B (no GUCs set) must NOT hit a slot populated under connection A\'s GUCs',
        );

        // Connection A still hits its own slot.
        $hitA = $cache->get($sql, null, $stateA->stateHash());
        $this->assertSame([['id' => 'A1']], $hitA['rows']);
    }

    public function testTwoConnectionsWithDifferentSetsDoNotShareSlots(): void
    {
        // Both connections do SETs, with different values. Each must see
        // only its own slot.
        $cache = $this->makeCache();
        $stateA = $this->makeState();
        $stateB = $this->makeState();

        $sql = 'SELECT * FROM rls_table';

        $stateA->observeSql("SET app.user_id = 'A'");
        $stateB->observeSql("SET app.user_id = 'B'");
        $this->assertNotSame($stateA->stateHash(), $stateB->stateHash());

        $cache->put($sql, null, [['id' => 'A-row']], ['id'], $stateA->stateHash());
        $cache->put($sql, null, [['id' => 'B-row']], ['id'], $stateB->stateHash());

        $this->assertSame([['id' => 'A-row']], $cache->get($sql, null, $stateA->stateHash())['rows']);
        $this->assertSame([['id' => 'B-row']], $cache->get($sql, null, $stateB->stateHash())['rows']);
    }

    // ─── bumpDmlSeq (Wave 5 — trigger-internal SET safety) ───────────────

    public function testBumpDmlSeqRollsHashForward(): void
    {
        // Fresh connection starts at the canonical "0" hash. A bump
        // rolls it forward — the post-DML read can't share a cache
        // slot with the pre-DML read on the same connection.
        $state = $this->makeState();
        $this->assertSame(0, $state->dmlSeq());
        $this->assertSame('0', $state->stateHash());

        $state->bumpDmlSeq();
        $this->assertSame(1, $state->dmlSeq());
        $this->assertNotSame('0', $state->stateHash());
    }

    public function testBumpDmlSeqSequentialBumpsAreDistinct(): void
    {
        // Each bump produces a fresh hash — a pre-DML cache slot
        // cannot leak forward across any number of writes.
        $state = $this->makeState();
        $hashes = [$state->stateHash()];
        for ($i = 0; $i < 5; $i++) {
            $state->bumpDmlSeq();
            $hashes[] = $state->stateHash();
        }
        $this->assertSame(count($hashes), count(array_unique($hashes)));
    }

    public function testBumpDmlSeqCombinesWithUnsafeSet(): void
    {
        // The bump mixes with the unsafe-GUC body — a connection with
        // an active SET AND a bump produces a distinct hash from a
        // peer with the same SET but no bump.
        $a = $this->makeState();
        $b = $this->makeState();
        $a->observeSql("SET app.user_id = '42'");
        $b->observeSql("SET app.user_id = '42'");
        $this->assertSame($a->stateHash(), $b->stateHash());

        $a->bumpDmlSeq();
        $this->assertNotSame($a->stateHash(), $b->stateHash());

        $b->bumpDmlSeq();
        $this->assertSame($a->stateHash(), $b->stateHash());
    }

    public function testBumpDmlSeqIsolatesFromPeerWithSameUnsafeGuc(): void
    {
        // Two connections with identical unsafe-GUC state — peer-
        // shareable cache slot. After one bumps, the bumped
        // connection drops out of the shared slot.
        $a = $this->makeState();
        $b = $this->makeState();
        $a->observeSql("SET app.user_id = '42'");
        $b->observeSql("SET app.user_id = '42'");
        $shared = $a->stateHash();

        $a->bumpDmlSeq();
        $this->assertNotSame($shared, $a->stateHash());
        $this->assertSame($shared, $b->stateHash());
    }

    public function testDiscardAllResetsDmlSeq(): void
    {
        // DISCARD ALL is the universal "server is in default state"
        // signal — clears dml_seq + state map so the connection
        // converges back to the peer-shareable baseline.
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $state->bumpDmlSeq();
        $state->bumpDmlSeq();
        $state->bumpDmlSeq();
        $this->assertSame(3, $state->dmlSeq());
        $this->assertNotSame('0', $state->stateHash());

        $state->observeSql('DISCARD ALL');
        $this->assertSame(0, $state->dmlSeq());
        $this->assertSame('0', $state->stateHash());
    }

    public function testResetAllResetsDmlSeq(): void
    {
        // RESET ALL is functionally identical to DISCARD ALL for the
        // unsafe-GUC subset — same dml_seq reset behaviour.
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $state->bumpDmlSeq();
        $state->observeSql('RESET ALL');
        $this->assertSame(0, $state->dmlSeq());
        $this->assertSame('0', $state->stateHash());
    }

    public function testResetNamedDoesNotClearDmlSeq(): void
    {
        // RESET <single GUC> only clears that one GUC — dml_seq
        // survives because the connection hasn't returned to default.
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $state->bumpDmlSeq();
        $state->observeSql('RESET app.user_id');
        $this->assertSame(1, $state->dmlSeq());
        $this->assertNotSame('0', $state->stateHash());
    }

    public function testBumpDmlSeqSurvivesSnapshotRoundTrip(): void
    {
        // Snapshot / restore preserves dml_seq. Critical for the
        // ROLLBACK + failed-SET recovery paths — a tx that bumped
        // dml_seq via an INSERT then rolled back keeps the bump (the
        // INSERT did execute, even if the tx aborted; we don't want
        // to share cache slots with a hypothetical clean state).
        $state = $this->makeState();
        $state->observeSql("SET app.user_id = '42'");
        $state->bumpDmlSeq();
        $state->bumpDmlSeq();
        $snap = $state->snapshot();

        $state->bumpDmlSeq();
        $this->assertSame(3, $state->dmlSeq());

        $state->restore($snap);
        $this->assertSame(2, $state->dmlSeq());
    }

    public function testDmlBumpForcesCacheMissAfterPreDmlHit(): void
    {
        // End-to-end: same SQL keyed under the pre-DML state hash hits;
        // the post-DML state hash misses (because the bump rolled the
        // hash forward). Closes the trigger-internal-SET gap at the
        // cache layer.
        $cache = $this->makeCache();
        $state = $this->makeState();
        $sql = 'SELECT * FROM accounts';

        // Pre-DML read populates a cache slot.
        $cache->put($sql, null, [['id' => 'pre']], ['id'], $state->stateHash());
        $this->assertSame(
            [['id' => 'pre']],
            $cache->get($sql, null, $state->stateHash())['rows'],
        );

        // Bump simulates the post-DML hash roll-forward.
        $state->bumpDmlSeq();

        // Same SQL under the new state hash MISSES — the pre-DML row
        // cannot leak across the trigger-internal-SET boundary.
        $this->assertNull($cache->get($sql, null, $state->stateHash()));
    }
}
