<?php

namespace GoldLapel\Tests;

use GoldLapel\Utils;
use PHPUnit\Framework\TestCase;

class UtilsTest extends TestCase
{
    private function makeMockPDO(): \PDO
    {
        return $this->createMock(\PDO::class);
    }

    private function makeMockStmt(array $rows): \PDOStatement
    {
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchAll')->willReturn($rows);
        $stmt->method('fetch')->willReturn($rows[0] ?? false);
        return $stmt;
    }

    // -- analyze --

    public function testAnalyzeReturnsRows(): void
    {
        $rows = [
            ['alias' => 'asciiword', 'description' => 'Word, all ASCII', 'token' => 'hello', 'dictionaries' => '{english_stem}', 'dictionary' => 'english_stem', 'lexemes' => '{hello}'],
            ['alias' => 'blank', 'description' => 'Space symbols', 'token' => ' ', 'dictionaries' => '{}', 'dictionary' => '', 'lexemes' => ''],
            ['alias' => 'asciiword', 'description' => 'Word, all ASCII', 'token' => 'world', 'dictionaries' => '{english_stem}', 'dictionary' => 'english_stem', 'lexemes' => '{world}'],
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with("SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug(?, ?)")
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with(['english', 'hello world']);

        $result = Utils::analyze($pdo, 'hello world');
        $this->assertCount(3, $result);
        $this->assertSame('asciiword', $result[0]['alias']);
        $this->assertSame('hello', $result[0]['token']);
    }

    public function testAnalyzeCustomLang(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with(['french', 'bonjour']);

        $result = Utils::analyze($pdo, 'bonjour', 'french');
        $this->assertSame([], $result);
    }

    public function testAnalyzeEmptyText(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with(['english', '']);

        $result = Utils::analyze($pdo, '');
        $this->assertSame([], $result);
    }

    // -- explainScore --

    public function testExplainScoreReturnsRow(): void
    {
        $row = [
            'document_text' => 'The quick brown fox jumps over the lazy dog',
            'document_tokens' => "'brown':3 'dog':9 'fox':4 'jump':5 'lazi':8 'quick':2",
            'query_tokens' => "'fox'",
            'matches' => true,
            'score' => 0.0607927,
            'headline' => 'The quick brown **fox** jumps over the lazy dog',
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->with(\PDO::FETCH_ASSOC)->willReturn($row);
        $pdo->expects($this->once())->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'fox', 'english', 'english', 'fox', 'english', 'english', 'fox', 'english', 'english', 'fox', 42]);

        $result = Utils::explainScore($pdo, 'articles', 'body', 'fox', 'id', 42);
        $this->assertIsArray($result);
        $this->assertSame('The quick brown fox jumps over the lazy dog', $result['document_text']);
        $this->assertTrue($result['matches']);
        $this->assertStringContainsString('**fox**', $result['headline']);
    }

    public function testExplainScoreReturnsNullWhenNoRow(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);
        $pdo->method('prepare')->willReturn($stmt);

        $result = Utils::explainScore($pdo, 'articles', 'body', 'nonexistent', 'id', 999);
        $this->assertNull($result);
    }

    public function testExplainScoreCustomLang(): void
    {
        $row = [
            'document_text' => 'Le renard brun rapide',
            'document_tokens' => "'renard':2",
            'query_tokens' => "'renard'",
            'matches' => true,
            'score' => 0.0607927,
            'headline' => 'Le **renard** brun rapide',
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn($row);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['french', 'french', 'renard', 'french', 'french', 'renard', 'french', 'french', 'renard', 'french', 'french', 'renard', 1]);

        $result = Utils::explainScore($pdo, 'articles', 'body', 'renard', 'id', 1, 'french');
        $this->assertIsArray($result);
        $this->assertSame('Le renard brun rapide', $result['document_text']);
    }

    public function testExplainScoreStringIdValue(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'test', 'english', 'english', 'test', 'english', 'english', 'test', 'english', 'english', 'test', 'abc-123']);

        Utils::explainScore($pdo, 'posts', 'content', 'test', 'slug', 'abc-123');
    }

    public function testExplainScoreInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::explainScore($pdo, 'bad table!', 'body', 'query', 'id', 1);
    }

    public function testExplainScoreInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::explainScore($pdo, 'articles', 'bad column', 'query', 'id', 1);
    }

    public function testExplainScoreInvalidIdColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::explainScore($pdo, 'articles', 'body', 'query', '1; DROP TABLE', 1);
    }

    public function testExplainScoreSqlStructure(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetch')->willReturn(false);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('title AS document_text', $sql);
                $this->assertStringContainsString('to_tsvector(?, title)::text AS document_tokens', $sql);
                $this->assertStringContainsString('plainto_tsquery(?, ?)::text AS query_tokens', $sql);
                $this->assertStringContainsString('to_tsvector(?, title) @@ plainto_tsquery(?, ?) AS matches', $sql);
                $this->assertStringContainsString('ts_rank(to_tsvector(?, title), plainto_tsquery(?, ?)) AS score', $sql);
                $this->assertStringContainsString("ts_headline(?, title, plainto_tsquery(?, ?), 'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline", $sql);
                $this->assertStringContainsString('FROM docs WHERE doc_id = ?', $sql);
                return true;
            }))
            ->willReturn($stmt);

        Utils::explainScore($pdo, 'docs', 'title', 'search', 'doc_id', 5);
    }
}
