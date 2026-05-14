<?php

namespace GoldLapel\Tests;

use GoldLapel\CachedPDO;
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

    // ─── DISCARD ALL clears dirty flag (state x verify interaction) ─────

    public function testDiscardAllClearsDirtyFlag(): void
    {
        // DISCARD ALL is the universal "server is in default state"
        // signal — even if the connection was dirty, after observing
        // the discard there's nothing to verify (state is empty).
        $state = new ConnectionGucState();
        $state->markDirty();
        $this->assertTrue($state->isDirty());
        $state->observeSql('DISCARD ALL');
        $this->assertFalse($state->isDirty());
    }

    // ─── 4. Verify-on-checkout fallback ─────────────────────────────────

    public function testFreshStateNotDirty(): void
    {
        $state = new ConnectionGucState();
        $this->assertFalse($state->isDirty());
    }

    public function testMarkDirtyToggles(): void
    {
        $state = new ConnectionGucState();
        $state->markDirty();
        $this->assertTrue($state->isDirty());
        $state->clearDirty();
        $this->assertFalse($state->isDirty());
    }

    public function testApplyVerifyResultClearsDirty(): void
    {
        $state = new ConnectionGucState();
        $state->markDirty();
        $state->applyVerifyResult([
            ['name' => 'app.user_id', 'setting' => '42'],
        ]);
        $this->assertFalse($state->isDirty());
    }

    public function testApplyVerifyResultRebuildsState(): void
    {
        $state = new ConnectionGucState();
        $state->markDirty();
        $state->applyVerifyResult([
            ['name' => 'app.user_id', 'setting' => '42'],
            ['name' => 'role', 'setting' => 'tenant'],
            // Safe GUCs in the input are filtered out.
            ['name' => 'application_name', 'setting' => 'whatever'],
            ['name' => 'work_mem', 'setting' => '4MB'],
        ]);
        $this->assertNotSame('0', $state->stateHash());

        // The reconstructed state hash must match what an in-band SET
        // pair would have produced.
        $other = new ConnectionGucState();
        $other->observeSql("SET app.user_id = '42'");
        $other->observeSql("SET role = 'tenant'");
        $this->assertSame($other->stateHash(), $state->stateHash());
    }

    public function testApplyVerifyResultEmptyInputResetsState(): void
    {
        // Server returned no session-level GUCs → state is back to default.
        $state = new ConnectionGucState();
        $state->observeSql("SET app.user_id = '42'");
        $state->markDirty();
        $state->applyVerifyResult([]);
        $this->assertSame('0', $state->stateHash());
        $this->assertFalse($state->isDirty());
    }

    public function testApplyVerifyResultPositionalRows(): void
    {
        // FETCH_NUM rows: numeric keys 0, 1.
        $state = new ConnectionGucState();
        $state->markDirty();
        $state->applyVerifyResult([
            [0 => 'app.user_id', 1 => '42'],
        ]);
        $this->assertNotSame('0', $state->stateHash());
    }

    public function testApplyVerifyResultMixedCaseKeys(): void
    {
        // Some PDO drivers preserve column-name casing — name lookup is
        // case-insensitive defensively.
        $state = new ConnectionGucState();
        $state->markDirty();
        $state->applyVerifyResult([
            ['Name' => 'app.user_id', 'Setting' => '42'],
        ]);
        $this->assertNotSame('0', $state->stateHash());
    }

    public function testApplyVerifyResultIgnoresMalformedRows(): void
    {
        $state = new ConnectionGucState();
        $state->markDirty();
        $state->applyVerifyResult([
            ['unrelated' => 'junk'],
            ['name' => 'app.user_id', 'setting' => '42'],
            [], // empty
            ['name' => null, 'setting' => null],
        ]);
        // Only the well-formed row was accepted.
        $other = new ConnectionGucState();
        $other->observeSql("SET app.user_id = '42'");
        $this->assertSame($other->stateHash(), $state->stateHash());
    }

    public function testCachedPdoDirtyFlagBypassesCacheRead(): void
    {
        // Wave 5: when dirty, the wrapper bypasses the L1 cache rather
        // than running a pg_settings reconcile query. Reads route
        // straight through; nothing is seeded into the cache.
        $userStmt = $this->createStub(\PDOStatement::class);
        $userStmt->method('fetchAll')->willReturn([['n' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        // Only the user's real query — no pg_settings probe.
        $pdo->expects($this->once())
            ->method('query')
            ->with('SELECT 1')
            ->willReturn($userStmt);

        $cache = new NativeCache();
        $cache->setConnected(true);
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $wrapper->query('SELECT 1');

        // Connection stays dirty (no implicit clear on read); cache
        // remained untouched.
        $this->assertTrue($wrapper->getGucState()->isDirty());
        $this->assertSame(0, $cache->size());
    }

    public function testCachedPdoDirtyFlagClearsOnDiscardAll(): void
    {
        // The dirty flag is cleared on DISCARD ALL — the universal
        // "server is in default state" signal. Subsequent reads
        // participate in L1 again.
        $discardStmt = $this->createStub(\PDOStatement::class);
        $discardStmt->method('fetchAll')->willReturn([]);
        $discardStmt->method('columnCount')->willReturn(0);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->with('DISCARD ALL')
            ->willReturn($discardStmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $this->assertTrue($wrapper->getGucState()->isDirty());

        $wrapper->query('DISCARD ALL');
        $this->assertFalse($wrapper->getGucState()->isDirty());
    }

    // ─── 5. Post-call verify on top-level SELECT <function>(...) ────────

    public function testIsTopLevelFunctionCallRecognisesCommonShapes(): void
    {
        $this->assertTrue(NativeCache::isTopLevelFunctionCall('SELECT my_handler()'));
        $this->assertTrue(NativeCache::isTopLevelFunctionCall("SELECT my_handler('x')"));
        $this->assertTrue(NativeCache::isTopLevelFunctionCall("SELECT my_handler(1, 2, 3)"));
        $this->assertTrue(NativeCache::isTopLevelFunctionCall("SELECT  my_handler( 1 ) "));
        $this->assertTrue(NativeCache::isTopLevelFunctionCall("SELECT my_handler(1);"));
        $this->assertTrue(NativeCache::isTopLevelFunctionCall("SELECT pg_catalog.now()"));
        // Underscored / dollar-suffixed identifiers are legal.
        $this->assertTrue(NativeCache::isTopLevelFunctionCall("SELECT _internal_handler\$1()"));
    }

    public function testIsTopLevelFunctionCallRejectsNonFunctionShapes(): void
    {
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('SELECT 1'));
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('SELECT * FROM users'));
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('SELECT col FROM t'));
        // FROM clause = real read, not a side-effect call.
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('SELECT count(*) FROM t'));
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('SELECT now() FROM t'));
        // Non-SELECT statements
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('UPDATE x SET y=1'));
        $this->assertFalse(NativeCache::isTopLevelFunctionCall('SET app.user_id = 1'));
        $this->assertFalse(NativeCache::isTopLevelFunctionCall(''));
    }

    public function testIsTopLevelFunctionCallExcludesSetConfig(): void
    {
        // set_config is captured inline by parseSetConfigCall — no
        // need to redundantly fire post-call verify for it.
        $this->assertFalse(
            NativeCache::isTopLevelFunctionCall("SELECT set_config('app.user_id', '42', false)"),
        );
        $this->assertFalse(
            NativeCache::isTopLevelFunctionCall("SELECT pg_catalog.set_config('a', 'b', false)"),
        );
    }

    public function testCachedPdoPostCallVerifyOnFunctionCall(): void
    {
        // Wave 5: SELECT my_handler() bumps the connection's dml_seq
        // counter (zero round-trips). Any subsequent cacheable read
        // routes to a different cache slot because the state hash
        // rolled forward, closing the function-internal-SET gap.
        $funcStmt = $this->createStub(\PDOStatement::class);
        $funcStmt->method('fetchAll')->willReturn([['my_handler' => null]]);

        $pdo = $this->createMock(\PDO::class);
        // ONLY the function call — no pg_settings follow-up probe.
        $pdo->expects($this->once())
            ->method('query')
            ->with('SELECT my_handler()')
            ->willReturn($funcStmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);

        $beforeSeq = $wrapper->getGucState()->dmlSeq();
        $beforeHash = $wrapper->getGucState()->stateHash();
        $stmt = $wrapper->query('SELECT my_handler()');
        $this->assertNotFalse($stmt);

        // dml_seq bumped; state hash rolled forward.
        $this->assertSame($beforeSeq + 1, $wrapper->getGucState()->dmlSeq());
        $this->assertNotSame($beforeHash, $wrapper->getGucState()->stateHash());
    }

    public function testCachedPdoPostCallVerifySkippedOnPlainSelect(): void
    {
        // SELECT * FROM users — no function call, no verify query fired.
        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([['id' => 1]]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->with('SELECT * FROM users')
            ->willReturn($stmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->query('SELECT * FROM users');
    }

    public function testCachedPdoPostCallVerifySkippedInsideTransaction(): void
    {
        // Inside an open transaction, post-call verify is suppressed —
        // see CachedPDO::query() rationale.
        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('beginTransaction')->willReturn(true);
        // Single call: the function execution, no verify follow-up.
        $pdo->expects($this->once())
            ->method('query')
            ->with('SELECT my_handler()')
            ->willReturn($stmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->beginTransaction();
        $wrapper->query('SELECT my_handler()');
    }

    public function testCachedPdoPostCallVerifyHonoursOffMode(): void
    {
        // Mode 'off' opts out of the dml_seq bump on function calls
        // too — the user accepted the correctness envelope shrink for
        // every path the wrapper would otherwise close.
        $funcStmt = $this->createStub(\PDOStatement::class);
        $funcStmt->method('fetchAll')->willReturn([['my_handler' => null]]);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->willReturn($funcStmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache, \GoldLapel\AggressiveVerify::MODE_OFF);
        @$wrapper->query('SELECT my_handler()');

        $this->assertSame(0, $wrapper->getGucState()->dmlSeq());
        $this->assertSame('0', $wrapper->getGucState()->stateHash());
    }

    public function testCachedPdoPostCallVerifyOnCacheHitSkipped(): void
    {
        // Pre-populate the cache for SELECT my_handler() so the next
        // call hits L1. The function did NOT execute server-side, so
        // there's no new state to verify — verify query must not fire.
        $cache = new NativeCache();
        $cache->setConnected(true);
        $cache->put('SELECT my_handler()', null, [['my_handler' => 'cached']], ['my_handler'], '0');

        $pdo = $this->createMock(\PDO::class);
        // Zero query invocations — verify must not fire on cache hit.
        $pdo->expects($this->never())->method('query');

        $wrapper = new CachedPDO($pdo, $cache);
        $stmt = $wrapper->query('SELECT my_handler()');
        $this->assertNotFalse($stmt);
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertSame([['my_handler' => 'cached']], $rows);
    }

    public function testCachedPdoExecPathPostCallVerify(): void
    {
        // exec() path also bumps dml_seq on a top-level fn call.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->with('SELECT my_handler()')
            ->willReturn(0);
        // No pg_settings probe — Wave 5 replaced it with the bump.
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->exec('SELECT my_handler()');

        $this->assertSame(1, $wrapper->getGucState()->dmlSeq());
        $this->assertNotSame('0', $wrapper->getGucState()->stateHash());
    }

    public function testCachedPdoExecPostCallVerifyMarksDirtyOnExecFailure(): void
    {
        // exec returns false — the function call may have partially
        // mutated state. Mark dirty for next acquire.
        $pdo = $this->createMock(\PDO::class);
        $pdo->method('exec')->willReturn(false);
        $pdo->expects($this->never())->method('query');

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->exec('SELECT my_handler()');
        $this->assertTrue($wrapper->getGucState()->isDirty());
    }

    public function testDirtyBypassDoesNotSeedCache(): void
    {
        // Each query() under a dirty connection routes through to the
        // real PDO. We never seed the cache while dirty (would be
        // unsafe: state map may not match server), so the next read
        // also misses.
        $userStmt = $this->createStub(\PDOStatement::class);
        $userStmt->method('fetchAll')->willReturn([['n' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->exactly(2))
            ->method('query')
            ->with('SELECT 1')
            ->willReturn($userStmt);

        $cache = new NativeCache();
        $cache->setConnected(true);
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $wrapper->query('SELECT 1');
        // Still dirty (markStateDirty isn't cleared by reads).
        $wrapper->query('SELECT 1');
    }

    // ─── End-to-end: persistent-PDO reuse safety ────────────────────────

    public function testReuseAfterDirtyDoesNotServeStaleCacheHit(): void
    {
        // Scenario: persistent PDO is reused. Previous request mutated
        // app.user_id to 'A' via a stored function (we never saw it).
        // Frontend calls markStateDirty before handing off. Next
        // request's first query() bypasses the cache because dirty —
        // a pre-existing cached row under the wrong state hash CANNOT
        // be served. The real PDO is called.
        $readStmt = $this->createStub(\PDOStatement::class);
        $readStmt->method('fetchAll')->willReturn([['id' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->with('SELECT * FROM accounts')
            ->willReturn($readStmt);

        $cache = new NativeCache();
        $cache->setConnected(true);
        // Pre-seed a hostile cache slot under the empty state hash,
        // mimicking a previous request that left behind a populated
        // cache before app.user_id was server-side SET to 'A'.
        $cache->put('SELECT * FROM accounts', null, [['id' => 999]], ['id'], '0');

        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $stmt = $wrapper->query('SELECT * FROM accounts');
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        // The dirty-bypass routed to the real PDO; the stale-state
        // cached row (id=999) was NOT served.
        $this->assertSame([['id' => 1]], $rows);
    }
}
