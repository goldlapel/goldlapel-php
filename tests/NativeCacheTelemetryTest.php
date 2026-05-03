<?php

namespace GoldLapel\Tests;

use GoldLapel\NativeCache;
use PHPUnit\Framework\TestCase;

/**
 * Tests for the L1 telemetry pattern ported from goldlapel-python.
 *
 * Two layers:
 *   - Unit-level: a test subclass overrides emitLine() to capture
 *     emissions in an array. Fast, no socket needed.
 *   - Integration-level: spin a real stream_socket_server on
 *     127.0.0.1:0, connect the cache, accept, read its lines, and
 *     assert on protocol shape.
 */
class NativeCacheTelemetryTest extends TestCase
{
    protected function setUp(): void
    {
        NativeCache::reset();
        // Force telemetry on for tests — the SAPI is already 'cli' under
        // PHPUnit so this is mostly belt-and-suspenders, but it keeps
        // the suite deterministic if anyone runs phpunit through a
        // non-standard SAPI.
        putenv('GOLDLAPEL_REPORT_STATS=true');
    }

    protected function tearDown(): void
    {
        NativeCache::reset();
        putenv('GOLDLAPEL_REPORT_STATS');
    }

    private function makeCapturingCache(?int $maxEntries = null): RecordingNativeCache
    {
        $cache = new RecordingNativeCache($maxEntries);
        $cache->setConnected(true);
        return $cache;
    }

    // ---- counters ----

    public function testEvictionsCounterStartsAtZero(): void
    {
        $cache = $this->makeCapturingCache(4);
        $this->assertSame(0, $cache->statsEvictions);
    }

    public function testEvictionsCounterBumpsOnOverflow(): void
    {
        $cache = $this->makeCapturingCache(4);
        for ($i = 0; $i < 8; $i++) {
            $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
        }
        // 8 puts, capacity 4 → 4 evictions.
        $this->assertSame(4, $cache->statsEvictions);
    }

    public function testEvictionsCounterStaysZeroWithinCapacity(): void
    {
        $cache = $this->makeCapturingCache(8);
        for ($i = 0; $i < 4; $i++) {
            $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
        }
        $this->assertSame(0, $cache->statsEvictions);
    }

    // ---- snapshot shape ----

    public function testSnapshotHasRequiredFields(): void
    {
        $cache = $this->makeCapturingCache(64);
        $cache->put('SELECT 1', null, [['x' => '1']], ['x']);
        $cache->get('SELECT 1', null);
        $cache->get('SELECT MISS', null);
        $snap = $cache->buildSnapshot();
        $this->assertSame($cache->getWrapperId(), $snap['wrapper_id']);
        $this->assertSame('php', $snap['lang']);
        $this->assertArrayHasKey('version', $snap);
        $this->assertSame(1, $snap['hits']);
        $this->assertSame(1, $snap['misses']);
        $this->assertSame(0, $snap['evictions']);
        $this->assertSame(0, $snap['invalidations']);
        $this->assertSame(1, $snap['current_size_entries']);
        $this->assertSame(64, $snap['capacity_entries']);
    }

    public function testWrapperIdIsUuidV4(): void
    {
        $cache = $this->makeCapturingCache();
        $id = $cache->getWrapperId();
        // RFC 4122 v4: xxxxxxxx-xxxx-4xxx-[89ab]xxx-xxxxxxxxxxxx
        $this->assertMatchesRegularExpression(
            '/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/',
            $id,
        );
    }

    public function testWrapperIdStableAcrossSnapshots(): void
    {
        $cache = $this->makeCapturingCache();
        $a = $cache->buildSnapshot()['wrapper_id'];
        $b = $cache->buildSnapshot()['wrapper_id'];
        $this->assertSame($a, $b);
    }

    // ---- ?:snapshot handling ----

    public function testProcessRequestSnapshotEmitsResponse(): void
    {
        $cache = $this->makeCapturingCache();
        $cache->processSignal('?:snapshot');
        $rLines = array_values(array_filter(
            $cache->emissions,
            fn($l) => str_starts_with($l, 'R:'),
        ));
        $this->assertCount(1, $rLines);
        $payload = json_decode(substr($rLines[0], 2), true);
        $this->assertSame($cache->getWrapperId(), $payload['wrapper_id']);
        $this->assertArrayHasKey('ts_ms', $payload);
    }

    public function testProcessRequestEmptyBodyTreatedAsSnapshot(): void
    {
        $cache = $this->makeCapturingCache();
        $cache->processSignal('?:');
        $rLines = array_values(array_filter(
            $cache->emissions,
            fn($l) => str_starts_with($l, 'R:'),
        ));
        $this->assertCount(1, $rLines);
    }

    public function testProcessRequestUnknownBodySilentlyDropped(): void
    {
        $cache = $this->makeCapturingCache();
        $cache->processSignal('?:future_request_type');
        $rLines = array_values(array_filter(
            $cache->emissions,
            fn($l) => str_starts_with($l, 'R:'),
        ));
        $this->assertSame([], $rLines);
    }

    public function testUnknownPrefixIgnored(): void
    {
        $cache = $this->makeCapturingCache();
        // Forward-compat: must not crash on unknown prefixes.
        $cache->processSignal('Z:future-prefix');
        $cache->processSignal('$:bogus');
        // Only emissions should be from connect/test setup (none here).
        $this->assertSame([], $cache->emissions);
    }

    // ---- eviction-rate state changes ----

    public function testCacheFullFiresWhenEvictionsDominate(): void
    {
        // Capacity 4 — every put past the 4th evicts. Window = 200 puts.
        $cache = $this->makeCapturingCache(4);
        for ($i = 0; $i < 210; $i++) {
            $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
        }
        $cacheFull = array_values(array_filter(
            $cache->emissions,
            fn($l) => str_contains($l, '"state":"cache_full"'),
        ));
        $this->assertNotEmpty($cacheFull, 'expected at least one cache_full emission');
    }

    public function testCacheFullDoesNotFireBeforeWindowFills(): void
    {
        $cache = $this->makeCapturingCache(2);
        // Fewer puts than the warmup window — no state change should fire.
        for ($i = 0; $i < 199; $i++) {
            $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
        }
        $cacheFull = array_values(array_filter(
            $cache->emissions,
            fn($l) => str_contains($l, '"state":"cache_full"'),
        ));
        $this->assertSame([], $cacheFull, 'cache_full must not fire before warmup window fills');
    }

    public function testCacheFullEmittedExactlyOnceWhileSustained(): void
    {
        $cache = $this->makeCapturingCache(4);
        for ($i = 0; $i < 400; $i++) {
            $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
        }
        $cacheFull = array_values(array_filter(
            $cache->emissions,
            fn($l) => str_contains($l, '"state":"cache_full"'),
        ));
        $this->assertCount(1, $cacheFull, 'latched flag must prevent re-emission while rate stays bad');
    }

    // ---- report_stats opt-out ----

    public function testReportStatsOptOutSuppressesAllEmissions(): void
    {
        putenv('GOLDLAPEL_REPORT_STATS=false');
        try {
            $cache = $this->makeCapturingCache(4);
            $this->assertFalse($cache->isReportingStats());
            for ($i = 0; $i < 210; $i++) {
                $cache->put("SELECT {$i}", null, [['x' => (string) $i]], ['x']);
            }
            $cache->processSignal('?:snapshot');
            $this->assertSame([], $cache->emissions);
        } finally {
            putenv('GOLDLAPEL_REPORT_STATS=true');
        }
    }

    public function testReportStatsDefaultFollowsSapi(): void
    {
        // PHP_SAPI is read-only at runtime so we can't synthesize an FPM
        // request inside PHPUnit. We test the contract instead: with no
        // env override, telemetry is on under CLI/phpdbg and off
        // everywhere else. Under FPM/Apache/CGI in production, this
        // resolves to false and the wrapper goes dark on the wire.
        putenv('GOLDLAPEL_REPORT_STATS');
        $cache = $this->makeCapturingCache();
        $expected = (PHP_SAPI === 'cli' || PHP_SAPI === 'phpdbg');
        $this->assertSame($expected, $cache->isReportingStats());
    }

    // ---- integration: real socket server ----

    public function testWrapperConnectedEmittedOnSocketConnect(): void
    {
        $cache = new NativeCache();

        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);
        $port = (int) explode(':', stream_socket_get_name($server, false))[1];
        // Block until the wrapper connects, then read what it sent.
        // Connect will drain any signals we pre-write — we don't write
        // any, so it just reads zero bytes and returns.
        $cache->connectInvalidation($port);
        $conn = stream_socket_accept($server, 2);
        $this->assertNotFalse($conn);
        // Give the wrapper a moment to flush the wrapper_connected line.
        // It was written during connectInvalidation() before we accepted,
        // so it should be in the kernel buffer already.
        stream_set_timeout($conn, 1);
        $line = fgets($conn);
        fclose($conn);
        fclose($server);
        $cache->disconnect();

        $this->assertNotFalse($line, 'expected wrapper to send a line on connect');
        $this->assertStringStartsWith('S:', $line);
        $payload = json_decode(substr(rtrim($line), 2), true);
        $this->assertSame('wrapper_connected', $payload['state']);
        $this->assertSame($cache->getWrapperId(), $payload['wrapper_id']);
        $this->assertSame('php', $payload['lang']);
        $this->assertArrayHasKey('ts_ms', $payload);
        $this->assertArrayHasKey('hits', $payload);
        $this->assertArrayHasKey('capacity_entries', $payload);
    }

    public function testSnapshotRequestProducesResponseLine(): void
    {
        // Coordinate the round-trip across a forked child:
        //   parent: connectInvalidation → pollSignals → disconnect
        //   child:  accept → write ?:snapshot → read until R: or EOF
        // The S:wrapper_connected line goes out during connectInvalidation.
        // The R:<json> line goes out during pollSignals (which calls
        // drainSignals which routes ?:snapshot through processRequest).
        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        $this->assertNotFalse($server);
        $port = (int) explode(':', stream_socket_get_name($server, false))[1];

        $pid = pcntl_fork();
        if ($pid === 0) {
            $conn = stream_socket_accept($server, 2);
            if ($conn) {
                // Write ?:snapshot and flush. The parent will pick it up
                // on its next pollSignals().
                fwrite($conn, "?:snapshot\n");
                fflush($conn);
                stream_set_timeout($conn, 2);
                $buf = '';
                $deadline = microtime(true) + 2.0;
                while (microtime(true) < $deadline) {
                    $chunk = fread($conn, 4096);
                    if ($chunk === false) {
                        break;
                    }
                    if ($chunk === '') {
                        $info = stream_get_meta_data($conn);
                        if ($info['eof']) {
                            break;
                        }
                        usleep(20000);
                        continue;
                    }
                    $buf .= $chunk;
                    if (preg_match('/(^|\n)R:.*\n/s', $buf)) {
                        break;
                    }
                }
                file_put_contents('/tmp/goldlapel-php-test-emit.txt', $buf);
                fclose($conn);
            }
            fclose($server);
            exit(0);
        }
        fclose($server);
        // Tiny pause so the child has time to enter accept().
        usleep(150000);

        $cache = new NativeCache();
        $cache->connectInvalidation($port);
        // Give the child a moment to write ?:snapshot, then poll.
        usleep(150000);
        $cache->pollSignals();
        // Pause so the child's read buffer can drain, then disconnect.
        // disconnect() emits wrapper_disconnected and closes the socket;
        // the child's loop exits on EOF.
        usleep(150000);
        $cache->disconnect();
        pcntl_waitpid($pid, $status);

        $captured = @file_get_contents('/tmp/goldlapel-php-test-emit.txt');
        @unlink('/tmp/goldlapel-php-test-emit.txt');
        $this->assertNotFalse($captured);

        $lines = array_filter(explode("\n", $captured));
        $rLines = array_values(array_filter($lines, fn($l) => str_starts_with($l, 'R:')));
        $this->assertNotEmpty($rLines, "expected R: line in: {$captured}");
        $payload = json_decode(substr($rLines[0], 2), true);
        $this->assertSame($cache->getWrapperId(), $payload['wrapper_id']);
        $this->assertSame('php', $payload['lang']);
    }
}

/**
 * Subclass that captures emitted lines instead of writing them to a
 * socket. Mirrors the Python pattern of monkey-patching `_send_line`
 * with a list.append.
 */
class RecordingNativeCache extends NativeCache
{
    public array $emissions = [];

    protected function emitLine(string $line): void
    {
        if (!$this->isReportingStats()) {
            return;
        }
        $this->emissions[] = rtrim($line, "\n");
    }
}
