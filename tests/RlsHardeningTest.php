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

    public function testCachedPdoVerifyIfDirtyOnCleanIsNoop(): void
    {
        // verifyIfDirty on a clean connection returns false without
        // touching the underlying PDO. We assert that with a PDO mock
        // that would throw if called.
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->never())->method('query');
        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $this->assertFalse($wrapper->verifyIfDirty());
    }

    public function testCachedPdoVerifyIfDirtyRunsAndClearsDirty(): void
    {
        // Stub a PDOStatement with one verify row.
        $stmt = $this->createStub(\PDOStatement::class);
        $stmt->method('fetchAll')->willReturn([
            ['name' => 'app.user_id', 'setting' => '42'],
        ]);
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_settings'))
            ->willReturn($stmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $this->assertTrue($wrapper->getGucState()->isDirty());

        $ok = $wrapper->verifyIfDirty();
        $this->assertTrue($ok);
        $this->assertFalse($wrapper->getGucState()->isDirty());
        $this->assertNotSame('0', $wrapper->getGucState()->stateHash());
    }

    public function testCachedPdoVerifyIfDirtyKeepsDirtyOnFailure(): void
    {
        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('query')
            ->willThrowException(new \PDOException('boom'));

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();

        $ok = $wrapper->verifyIfDirty();
        $this->assertFalse($ok);
        // Connection stays dirty for next acquire.
        $this->assertTrue($wrapper->getGucState()->isDirty());
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
        // After SELECT my_handler(), the wrapper issues the verify query
        // and applies its result. We stub the PDO so the function call
        // returns one row, AND the follow-up pg_settings query returns
        // a server-state row that the wrapper folds into state.
        $funcStmt = $this->createStub(\PDOStatement::class);
        $funcStmt->method('fetchAll')->willReturn([['my_handler' => null]]);

        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.user_id', 'setting' => '99'],
        ]);

        $pdo = $this->createMock(\PDO::class);
        $matcher = $this->exactly(2);
        $pdo->expects($matcher)
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($matcher, $funcStmt, $verifyStmt) {
                if ($matcher->numberOfInvocations() === 1) {
                    $this->assertSame('SELECT my_handler()', $sql);
                    return $funcStmt;
                }
                $this->assertStringContainsString('pg_settings', $sql);
                return $verifyStmt;
            });

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);

        $stmt = $wrapper->query('SELECT my_handler()');
        $this->assertNotFalse($stmt);

        // After the function call + verify, state reflects the
        // server-side mutation we never observed on the wire.
        $other = new ConnectionGucState();
        $other->observeSql("SET app.user_id = '99'");
        $this->assertSame($other->stateHash(), $wrapper->getGucState()->stateHash());
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

    public function testCachedPdoPostCallVerifyMarksDirtyOnFailure(): void
    {
        // Function executes OK but pg_settings query throws — wrapper
        // marks the connection dirty for next acquire instead of
        // bubbling the error to the user.
        $funcStmt = $this->createStub(\PDOStatement::class);
        $funcStmt->method('fetchAll')->willReturn([['my_handler' => null]]);

        $pdo = $this->createMock(\PDO::class);
        $matcher = $this->exactly(2);
        $pdo->expects($matcher)
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($matcher, $funcStmt) {
                if ($matcher->numberOfInvocations() === 1) {
                    return $funcStmt;
                }
                throw new \PDOException('proxy ate it');
            });

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $stmt = $wrapper->query('SELECT my_handler()');

        $this->assertNotFalse($stmt);
        $this->assertTrue($wrapper->getGucState()->isDirty());
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
        // exec() path also triggers post-call verify on top-level fn call.
        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.tenant', 'setting' => 'acme'],
        ]);

        $pdo = $this->createMock(\PDO::class);
        $pdo->expects($this->once())
            ->method('exec')
            ->with('SELECT my_handler()')
            ->willReturn(0);
        $pdo->expects($this->once())
            ->method('query')
            ->with($this->stringContains('pg_settings'))
            ->willReturn($verifyStmt);

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->exec('SELECT my_handler()');

        $other = new ConnectionGucState();
        $other->observeSql("SET app.tenant = 'acme'");
        $this->assertSame($other->stateHash(), $wrapper->getGucState()->stateHash());
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

    public function testVerifyDoesNotRefireAfterSuccessfulVerify(): void
    {
        // After a successful verifyIfDirty(), subsequent query() calls
        // must NOT re-issue the pg_settings query (the connection is
        // clean again).
        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.user_id', 'setting' => '7'],
        ]);
        $userStmt = $this->createStub(\PDOStatement::class);
        $userStmt->method('fetchAll')->willReturn([['n' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        // Expect: 1 verify + 2 user queries = 3 total.
        $matcher = $this->exactly(3);
        $pdo->expects($matcher)
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($matcher, $verifyStmt, $userStmt) {
                if ($matcher->numberOfInvocations() === 1) {
                    $this->assertStringContainsString('pg_settings', $sql);
                    return $verifyStmt;
                }
                $this->assertSame('SELECT 1', $sql);
                return $userStmt;
            });

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $wrapper->query('SELECT 1');
        // No new dirty mark — second query goes straight to user SQL.
        $wrapper->query('SELECT 1');
    }

    public function testQueryRunsVerifyIfDirtyBeforeSetObservation(): void
    {
        // Acquire-on-reuse: connection is dirty when query() is called,
        // verify query fires first, then the user's read runs against
        // the now-trustworthy state hash.
        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.user_id', 'setting' => '7'],
        ]);
        $userStmt = $this->createStub(\PDOStatement::class);
        $userStmt->method('fetchAll')->willReturn([['n' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        $matcher = $this->exactly(2);
        $pdo->expects($matcher)
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($matcher, $verifyStmt, $userStmt) {
                if ($matcher->numberOfInvocations() === 1) {
                    $this->assertStringContainsString('pg_settings', $sql);
                    return $verifyStmt;
                }
                $this->assertSame('SELECT 1', $sql);
                return $userStmt;
            });

        $cache = new NativeCache();
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $wrapper->query('SELECT 1');

        // Reconstructed state from the verify query is reflected in
        // the connection's hash now.
        $other = new ConnectionGucState();
        $other->observeSql("SET app.user_id = '7'");
        $this->assertSame($other->stateHash(), $wrapper->getGucState()->stateHash());
    }

    // ─── End-to-end: SET interleaved with verify-on-reuse ───────────────

    public function testReuseAfterDirtyDoesNotLeakStaleHash(): void
    {
        // Scenario: persistent PDO is reused. Previous request mutated
        // app.user_id to 'A' via a stored function (we never saw it).
        // Frontend calls markStateDirty before handing off. Next
        // request's first query() runs the verify, picks up app.user_id='A',
        // then reads safely under that fingerprint.
        $verifyStmt = $this->createStub(\PDOStatement::class);
        $verifyStmt->method('fetchAll')->willReturn([
            ['name' => 'app.user_id', 'setting' => 'A'],
        ]);
        $readStmt = $this->createStub(\PDOStatement::class);
        $readStmt->method('fetchAll')->willReturn([['id' => 1]]);

        $pdo = $this->createMock(\PDO::class);
        $matcher = $this->exactly(2);
        $pdo->expects($matcher)
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($matcher, $verifyStmt, $readStmt) {
                if ($matcher->numberOfInvocations() === 1) {
                    $this->assertStringContainsString('pg_settings', $sql);
                    return $verifyStmt;
                }
                $this->assertSame('SELECT * FROM accounts', $sql);
                return $readStmt;
            });

        $cache = new NativeCache();
        $cache->setConnected(true);
        $wrapper = new CachedPDO($pdo, $cache);
        $wrapper->markStateDirty();
        $wrapper->query('SELECT * FROM accounts');

        // The cached row is keyed under user-A's reconstructed state.
        $other = new ConnectionGucState();
        $other->observeSql("SET app.user_id = 'A'");
        $hit = $cache->get('SELECT * FROM accounts', null, $other->stateHash());
        $this->assertNotNull($hit);
        $this->assertSame([['id' => 1]], $hit['rows']);
    }
}
