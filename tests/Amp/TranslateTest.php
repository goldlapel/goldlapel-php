<?php

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\Utils;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for Utils::translate — the `?` to `$N` placeholder rewriter.
 * No Postgres required; pure string manipulation.
 */
class TranslateTest extends TestCase
{
    public function testEmptyParamsPassThrough(): void
    {
        [$sql, $p] = Utils::translate("SELECT 1", []);
        $this->assertSame("SELECT 1", $sql);
        $this->assertSame([], $p);
    }

    public function testSimplePlaceholder(): void
    {
        [$sql, $p] = Utils::translate("SELECT * FROM t WHERE id = ?", [42]);
        $this->assertSame("SELECT * FROM t WHERE id = \$1", $sql);
        $this->assertSame([42], $p);
    }

    public function testMultiplePlaceholders(): void
    {
        [$sql, $p] = Utils::translate(
            "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?",
            ['x', 'y', 'z']
        );
        $this->assertSame(
            "SELECT * FROM t WHERE a = \$1 AND b = \$2 AND c = \$3",
            $sql
        );
        $this->assertSame(['x', 'y', 'z'], $p);
    }

    public function testSingleQuotedStringIgnored(): void
    {
        // `?` inside a string literal is preserved — only bare `?` becomes $N
        [$sql, $p] = Utils::translate(
            "SELECT '?' AS q, id FROM t WHERE name = ?",
            ['alice']
        );
        $this->assertSame("SELECT '?' AS q, id FROM t WHERE name = \$1", $sql);
        $this->assertSame(['alice'], $p);
    }

    public function testEscapedSingleQuote(): void
    {
        // `''` is an escaped single quote, still inside the string
        [$sql, $p] = Utils::translate(
            "SELECT 'it''s ?' AS q WHERE id = ?",
            [7]
        );
        $this->assertSame("SELECT 'it''s ?' AS q WHERE id = \$1", $sql);
    }

    public function testDoubleQuotedIdentifierIgnored(): void
    {
        // Postgres uses `"name"` for identifiers with special chars; `?`
        // inside stays literal.
        [$sql, $p] = Utils::translate(
            'SELECT "col?name" FROM t WHERE id = ?',
            [1]
        );
        $this->assertSame('SELECT "col?name" FROM t WHERE id = $1', $sql);
    }

    public function testLineCommentIgnored(): void
    {
        [$sql, $p] = Utils::translate(
            "SELECT ? -- ? this is a comment\nFROM t",
            ['v']
        );
        $this->assertSame("SELECT \$1 -- ? this is a comment\nFROM t", $sql);
    }

    public function testBlockCommentIgnored(): void
    {
        [$sql, $p] = Utils::translate(
            "SELECT /* ? */ ? FROM t",
            ['v']
        );
        $this->assertSame("SELECT /* ? */ \$1 FROM t", $sql);
    }

    public function testDollarQuotedBodyIgnored(): void
    {
        // pllua bodies in script() — the `?` inside must stay literal
        $sql = 'CREATE FUNCTION f() RETURNS text LANGUAGE pllua AS $pllua$ return "?" $pllua$';
        [$out, $p] = Utils::translate($sql, []);
        $this->assertSame($sql, $out);
    }

    public function testJsonbExistsOperatorRewritten(): void
    {
        // buildFilter `$exists` emits `data ? ?` with ONE param (the key)
        [$sql, $p] = Utils::translate(
            "SELECT _id FROM coll WHERE data ? ?",
            ['email']
        );
        $this->assertSame(
            "SELECT _id FROM coll WHERE jsonb_exists(data, \$1)",
            $sql
        );
        $this->assertSame(['email'], $p);
    }

    public function testJsonbExistsInNotOperator(): void
    {
        [$sql, $p] = Utils::translate(
            "SELECT _id FROM coll WHERE NOT (data ? ?)",
            ['email']
        );
        $this->assertSame(
            "SELECT _id FROM coll WHERE NOT (jsonb_exists(data, \$1))",
            $sql
        );
    }

    public function testHdelPattern(): void
    {
        // Real sync hdel SQL — note `data ? ?` + another `?` for the key
        [$sql, $p] = Utils::translate(
            "SELECT data ? ? AS existed FROM t WHERE key = ?",
            ['field', 'key1']
        );
        $this->assertSame(
            "SELECT jsonb_exists(data, \$1) AS existed FROM t WHERE key = \$2",
            $sql
        );
        $this->assertSame(['field', 'key1'], $p);
    }

    public function testMismatchedParamCountThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::translate("SELECT * FROM t WHERE a = ? AND b = ?", ['one']);
    }

    public function testTooManyParamsThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        Utils::translate("SELECT ?", ['a', 'b']);
    }

    public function testDollarSignFollowedByLetterNotPlaceholder(): void
    {
        // Our SQL shouldn't produce this, but ensure we don't corrupt it
        [$sql, $p] = Utils::translate("SELECT * FROM t WHERE a = ?", [1]);
        $this->assertStringContainsString("\$1", $sql);
    }
}
