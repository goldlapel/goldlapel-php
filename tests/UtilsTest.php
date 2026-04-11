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

    // ========================================================================
    // search
    // ========================================================================

    public function testSearchSingleColumn(): void
    {
        $rows = [['id' => 1, 'title' => 'Hello World', '_score' => 0.06]];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("to_tsvector(?, coalesce(title, ''))", $sql);
                $this->assertStringContainsString('plainto_tsquery(?, ?)', $sql);
                $this->assertStringContainsString('ts_rank(', $sql);
                $this->assertStringContainsString('ORDER BY _score DESC LIMIT ?', $sql);
                $this->assertStringNotContainsString('ts_headline', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'hello', 'english', 'english', 'hello', 50]);

        $result = Utils::search($pdo, 'articles', 'title', 'hello');
        $this->assertCount(1, $result);
        $this->assertSame(1, $result[0]['id']);
    }

    public function testSearchMultiColumnCoalesce(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("coalesce(title, '') || ' ' || coalesce(body, '')", $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'test', 'english', 'english', 'test', 50]);

        Utils::search($pdo, 'articles', ['title', 'body'], 'test');
    }

    public function testSearchWithHighlight(): void
    {
        $rows = [['id' => 1, '_score' => 0.06, '_highlight' => '<mark>hello</mark> world']];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('ts_headline', $sql);
                $this->assertStringContainsString('StartSel=<mark>, StopSel=</mark>', $sql);
                $this->assertStringContainsString('AS _highlight', $sql);
                return true;
            }))
            ->willReturn($stmt);
        // highlight=true: ts_rank(tsv[lang], tsq[lang,query]) + ts_headline(lang, tsq[lang,query]) + WHERE tsv[lang] @@ tsq[lang,query] + LIMIT
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'hello', 'english', 'english', 'hello', 'english', 'english', 'hello', 50]);

        $result = Utils::search($pdo, 'articles', 'title', 'hello', highlight: true);
        $this->assertCount(1, $result);
        $this->assertStringContainsString('<mark>', $result[0]['_highlight']);
    }

    public function testSearchCustomLimitAndLang(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['french', 'french', 'bonjour', 'french', 'french', 'bonjour', 10]);

        Utils::search($pdo, 'articles', 'title', 'bonjour', limit: 10, lang: 'french');
    }

    public function testSearchInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::search($pdo, 'bad table!', 'title', 'query');
    }

    public function testSearchInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::search($pdo, 'articles', 'bad col!', 'query');
    }

    public function testSearchInvalidColumnInArray(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::search($pdo, 'articles', ['title', 'bad col!'], 'query');
    }

    // ========================================================================
    // searchFuzzy
    // ========================================================================

    public function testSearchFuzzy(): void
    {
        $rows = [['id' => 1, 'name' => 'hello', '_score' => 0.8]];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->never())->method('exec');
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('similarity(name, ?)', $sql);
                $this->assertStringContainsString('WHERE similarity(name, ?) > ?', $sql);
                $this->assertStringContainsString('ORDER BY _score DESC', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['helo', 'helo', 0.3, 50]);

        $result = Utils::searchFuzzy($pdo, 'users', 'name', 'helo');
        $this->assertCount(1, $result);
    }

    public function testSearchFuzzyCustomThresholdAndLimit(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['helo', 'helo', 0.5, 20]);

        Utils::searchFuzzy($pdo, 'users', 'name', 'helo', limit: 20, threshold: 0.5);
    }

    public function testSearchFuzzyInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::searchFuzzy($pdo, 'bad table', 'name', 'query');
    }

    public function testSearchFuzzyInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::searchFuzzy($pdo, 'users', 'bad col', 'query');
    }

    // ========================================================================
    // searchPhonetic
    // ========================================================================

    public function testSearchPhonetic(): void
    {
        $rows = [['id' => 1, 'name' => 'Smith', '_score' => 0.6]];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->never())->method('exec');
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('similarity(name, ?)', $sql);
                $this->assertStringContainsString('soundex(name) = soundex(?)', $sql);
                $this->assertStringContainsString('ORDER BY _score DESC, name', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['Smyth', 'Smyth', 50]);

        $result = Utils::searchPhonetic($pdo, 'users', 'name', 'Smyth');
        $this->assertCount(1, $result);
    }

    public function testSearchPhoneticCustomLimit(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['Jones', 'Jones', 10]);

        Utils::searchPhonetic($pdo, 'contacts', 'last_name', 'Jones', limit: 10);
    }

    public function testSearchPhoneticInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::searchPhonetic($pdo, '1bad', 'name', 'query');
    }

    public function testSearchPhoneticInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::searchPhonetic($pdo, 'users', '1bad', 'query');
    }

    // ========================================================================
    // similar
    // ========================================================================

    public function testSimilar(): void
    {
        $rows = [['id' => 1, '_score' => 0.12]];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->never())->method('exec');
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('embedding <=> ?::vector', $sql);
                $this->assertStringContainsString('AS _score', $sql);
                $this->assertStringContainsString('ORDER BY _score', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['[0.1,0.2,0.3]', 10]);

        $result = Utils::similar($pdo, 'documents', 'embedding', [0.1, 0.2, 0.3]);
        $this->assertCount(1, $result);
    }

    public function testSimilarCustomLimit(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['[1,2]', 5]);

        Utils::similar($pdo, 'docs', 'vec', [1, 2], limit: 5);
    }

    public function testSimilarVectorLiteral(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with($this->callback(function (array $params) {
                $this->assertSame('[0.5,0.6,0.7,0.8]', $params[0]);
                return true;
            }));

        Utils::similar($pdo, 'docs', 'vec', [0.5, 0.6, 0.7, 0.8]);
    }

    public function testSimilarInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::similar($pdo, 'bad table', 'vec', [1, 2, 3]);
    }

    public function testSimilarInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::similar($pdo, 'docs', 'bad col', [1, 2, 3]);
    }

    // ========================================================================
    // suggest
    // ========================================================================

    public function testSuggest(): void
    {
        $rows = [['id' => 1, 'name' => 'javascript', '_score' => 0.4]];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->never())->method('exec');
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('similarity(name, ?)', $sql);
                $this->assertStringContainsString('WHERE name ILIKE ?', $sql);
                $this->assertStringContainsString('ORDER BY _score DESC, name', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['java', 'java%', 10]);

        $result = Utils::suggest($pdo, 'tags', 'name', 'java');
        $this->assertCount(1, $result);
    }

    public function testSuggestCustomLimit(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['py', 'py%', 5]);

        Utils::suggest($pdo, 'tags', 'name', 'py', limit: 5);
    }

    public function testSuggestPrefixAppended(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['abc', 'abc%', 10]);

        Utils::suggest($pdo, 'tags', 'name', 'abc');
    }

    public function testSuggestInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::suggest($pdo, 'bad table', 'name', 'abc');
    }

    public function testSuggestInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::suggest($pdo, 'tags', 'bad col', 'abc');
    }

    // ========================================================================
    // facets
    // ========================================================================

    public function testFacetsWithoutQuery(): void
    {
        $rows = [
            ['value' => 'electronics', 'count' => 42],
            ['value' => 'books', 'count' => 17],
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('category AS value', $sql);
                $this->assertStringContainsString('COUNT(*) AS count', $sql);
                $this->assertStringContainsString('FROM products', $sql);
                $this->assertStringContainsString('GROUP BY category', $sql);
                $this->assertStringContainsString('ORDER BY count DESC, category', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                $this->assertStringNotContainsString('WHERE', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with([50]);

        $result = Utils::facets($pdo, 'products', 'category');
        $this->assertCount(2, $result);
        $this->assertSame('electronics', $result[0]['value']);
    }

    public function testFacetsWithQuerySingleColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("to_tsvector(?, coalesce(title, ''))", $sql);
                $this->assertStringContainsString('plainto_tsquery(?, ?)', $sql);
                $this->assertStringContainsString('WHERE', $sql);
                $this->assertStringContainsString('GROUP BY category', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'laptop', 50]);

        Utils::facets($pdo, 'products', 'category', query: 'laptop', queryColumn: 'title');
    }

    public function testFacetsWithQueryMultiColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString("coalesce(title, '') || ' ' || coalesce(body, '')", $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'english', 'laptop', 50]);

        Utils::facets($pdo, 'products', 'category', query: 'laptop', queryColumn: ['title', 'body']);
    }

    public function testFacetsCustomLimitAndLang(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['french', 'french', 'ordinateur', 20]);

        Utils::facets($pdo, 'products', 'category', limit: 20, query: 'ordinateur', queryColumn: 'title', lang: 'french');
    }

    public function testFacetsInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::facets($pdo, 'bad table', 'category');
    }

    public function testFacetsInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::facets($pdo, 'products', 'bad col');
    }

    public function testFacetsInvalidQueryColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::facets($pdo, 'products', 'category', query: 'test', queryColumn: 'bad col');
    }

    // ========================================================================
    // aggregate
    // ========================================================================

    public function testAggregateCountNoGroup(): void
    {
        $rows = [['value' => 42]];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('COUNT(*) AS value', $sql);
                $this->assertStringContainsString('FROM products', $sql);
                $this->assertStringNotContainsString('GROUP BY', $sql);
                $this->assertStringNotContainsString('LIMIT', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute');

        $result = Utils::aggregate($pdo, 'products', 'id', 'count');
        $this->assertCount(1, $result);
        $this->assertSame(42, $result[0]['value']);
    }

    public function testAggregateSumWithGroup(): void
    {
        $rows = [
            ['category' => 'electronics', 'value' => 500],
            ['category' => 'books', 'value' => 120],
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('SUM(price) AS value', $sql);
                $this->assertStringContainsString('GROUP BY category', $sql);
                $this->assertStringContainsString('ORDER BY value DESC', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with([50]);

        $result = Utils::aggregate($pdo, 'products', 'price', 'sum', groupBy: 'category');
        $this->assertCount(2, $result);
    }

    public function testAggregateAvg(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['value' => 25.5]]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('AVG(price) AS value', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->method('execute');

        Utils::aggregate($pdo, 'products', 'price', 'avg');
    }

    public function testAggregateMin(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['value' => 5]]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('MIN(price) AS value', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->method('execute');

        Utils::aggregate($pdo, 'products', 'price', 'min');
    }

    public function testAggregateMax(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['value' => 999]]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('MAX(price) AS value', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->method('execute');

        Utils::aggregate($pdo, 'products', 'price', 'max');
    }

    public function testAggregateCountUsesCountStar(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([['value' => 10]]);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('COUNT(*)', $sql);
                $this->assertStringNotContainsString('COUNT(id)', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->method('execute');

        Utils::aggregate($pdo, 'products', 'id', 'count');
    }

    public function testAggregateInvalidFunc(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid aggregate function: median');
        Utils::aggregate($pdo, 'products', 'price', 'median');
    }

    public function testAggregateInvalidTable(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::aggregate($pdo, 'bad table', 'price', 'sum');
    }

    public function testAggregateInvalidColumn(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::aggregate($pdo, 'products', 'bad col', 'sum');
    }

    public function testAggregateInvalidGroupBy(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid identifier');
        Utils::aggregate($pdo, 'products', 'price', 'sum', groupBy: 'bad col');
    }

    public function testAggregateCustomLimit(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with([5]);

        Utils::aggregate($pdo, 'products', 'price', 'sum', groupBy: 'category', limit: 5);
    }

    // ========================================================================
    // createSearchConfig
    // ========================================================================

    public function testCreateSearchConfigNew(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $stmt->method('fetchColumn')->willReturn(false);
        $pdo->expects($this->once())->method('prepare')
            ->with('SELECT 1 FROM pg_ts_config WHERE cfgname = ?')
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with(['my_config']);
        $pdo->expects($this->once())->method('exec')
            ->with('CREATE TEXT SEARCH CONFIGURATION my_config (COPY = english)');

        Utils::createSearchConfig($pdo, 'my_config');
    }

    public function testCreateSearchConfigAlreadyExists(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(1);
        $pdo->expects($this->once())->method('prepare')->willReturn($stmt);
        // exec should NOT be called if config already exists
        $pdo->expects($this->never())->method('exec');

        Utils::createSearchConfig($pdo, 'my_config');
    }

    public function testCreateSearchConfigCustomCopyFrom(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('fetchColumn')->willReturn(false);
        $pdo->method('prepare')->willReturn($stmt);
        $pdo->expects($this->once())->method('exec')
            ->with('CREATE TEXT SEARCH CONFIGURATION custom_cfg (COPY = french)');

        Utils::createSearchConfig($pdo, 'custom_cfg', 'french');
    }

    public function testCreateSearchConfigInvalidName(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::createSearchConfig($pdo, 'bad name!');
    }

    public function testCreateSearchConfigInvalidCopyFrom(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::createSearchConfig($pdo, 'my_config', 'bad copy!');
    }

    // ========================================================================
    // percolateAdd
    // ========================================================================

    public function testPercolateAdd(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);

        $execCalls = [];
        $pdo->method('exec')->willReturnCallback(function ($sql) use (&$execCalls) {
            $execCalls[] = $sql;
            return 0;
        });

        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('INSERT INTO alerts', $sql);
                $this->assertStringContainsString('plainto_tsquery(?, ?)', $sql);
                $this->assertStringContainsString('ON CONFLICT (query_id) DO UPDATE', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['q1', 'breaking news', 'english', 'breaking news', 'english', null]);

        Utils::percolateAdd($pdo, 'alerts', 'q1', 'breaking news');

        // Should create table and index
        $this->assertCount(2, $execCalls);
        $this->assertStringContainsString('CREATE TABLE IF NOT EXISTS alerts', $execCalls[0]);
        $this->assertStringContainsString('CREATE INDEX IF NOT EXISTS alerts_tsq_idx', $execCalls[1]);
    }

    public function testPercolateAddWithMetadata(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('exec');
        $pdo->method('prepare')->willReturn($stmt);
        $meta = ['user_id' => 42, 'channel' => 'email'];
        $stmt->expects($this->once())->method('execute')
            ->with(['q2', 'sports update', 'english', 'sports update', 'english', json_encode($meta)]);

        Utils::percolateAdd($pdo, 'alerts', 'q2', 'sports update', metadata: $meta);
    }

    public function testPercolateAddCustomLang(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('exec');
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['q3', 'nouvelles', 'french', 'nouvelles', 'french', null]);

        Utils::percolateAdd($pdo, 'alerts', 'q3', 'nouvelles', lang: 'french');
    }

    public function testPercolateAddInvalidName(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::percolateAdd($pdo, 'bad name!', 'q1', 'test');
    }

    // ========================================================================
    // percolate
    // ========================================================================

    public function testPercolate(): void
    {
        $rows = [
            ['query_id' => 'q1', 'query_text' => 'breaking news', 'metadata' => null, '_score' => 0.06],
        ];

        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt($rows);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('ts_rank(to_tsvector(?, ?), tsquery)', $sql);
                $this->assertStringContainsString('WHERE to_tsvector(?, ?) @@ tsquery', $sql);
                $this->assertStringContainsString('FROM alerts', $sql);
                $this->assertStringContainsString('ORDER BY _score DESC', $sql);
                $this->assertStringContainsString('LIMIT ?', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['english', 'Breaking news about tech', 'english', 'Breaking news about tech', 50]);

        $result = Utils::percolate($pdo, 'alerts', 'Breaking news about tech');
        $this->assertCount(1, $result);
        $this->assertSame('q1', $result[0]['query_id']);
    }

    public function testPercolateCustomLimitAndLang(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->makeMockStmt([]);
        $pdo->method('prepare')->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')
            ->with(['french', 'Les nouvelles du monde', 'french', 'Les nouvelles du monde', 10]);

        Utils::percolate($pdo, 'alerts', 'Les nouvelles du monde', limit: 10, lang: 'french');
    }

    public function testPercolateInvalidName(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::percolate($pdo, 'bad name!', 'test text');
    }

    // ========================================================================
    // percolateDelete
    // ========================================================================

    public function testPercolateDeleteSuccess(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(1);
        $pdo->expects($this->once())->method('prepare')
            ->with($this->callback(function (string $sql) {
                $this->assertStringContainsString('DELETE FROM alerts', $sql);
                $this->assertStringContainsString('WHERE query_id = ?', $sql);
                $this->assertStringContainsString('RETURNING query_id', $sql);
                return true;
            }))
            ->willReturn($stmt);
        $stmt->expects($this->once())->method('execute')->with(['q1']);

        $result = Utils::percolateDelete($pdo, 'alerts', 'q1');
        $this->assertTrue($result);
    }

    public function testPercolateDeleteNotFound(): void
    {
        $pdo = $this->makeMockPDO();
        $stmt = $this->createMock(\PDOStatement::class);
        $stmt->method('execute')->willReturn(true);
        $stmt->method('rowCount')->willReturn(0);
        $pdo->method('prepare')->willReturn($stmt);

        $result = Utils::percolateDelete($pdo, 'alerts', 'nonexistent');
        $this->assertFalse($result);
    }

    public function testPercolateDeleteInvalidName(): void
    {
        $pdo = $this->makeMockPDO();
        $this->expectException(\InvalidArgumentException::class);
        Utils::percolateDelete($pdo, 'bad name!', 'q1');
    }

    // ========================================================================
    // analyze
    // ========================================================================

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

    // ========================================================================
    // explainScore
    // ========================================================================

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
