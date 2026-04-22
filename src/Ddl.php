<?php
declare(strict_types=1);

namespace GoldLapel;

/**
 * DDL API client — fetches canonical helper-table DDL + query patterns from
 * the Rust proxy's dashboard port so the wrapper never hand-writes CREATE
 * TABLE for helper families (streams, docs, counters, ...).
 *
 * Architecture: see docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md in the goldlapel repo.
 *
 * - One HTTP call per (family, name) per session (cached).
 * - Cache lives on the GoldLapel instance (passed in by reference).
 * - Errors: HTTP failures throw RuntimeException with actionable text.
 *
 * PHP-FPM / shared-nothing: each request gets a fresh GoldLapel instance
 * and therefore a fresh cache. One extra HTTP round-trip per helper per
 * request — sub-millisecond to localhost.
 */
final class Ddl
{
    private const SUPPORTED_VERSIONS = [
        'stream' => 'v1',
    ];

    public static function supportedVersion(string $family): string
    {
        if (!isset(self::SUPPORTED_VERSIONS[$family])) {
            throw new \InvalidArgumentException("Unknown helper family: {$family}");
        }
        return self::SUPPORTED_VERSIONS[$family];
    }

    public static function tokenFromEnvOrFile(): ?string
    {
        $env = getenv('GOLDLAPEL_DASHBOARD_TOKEN');
        if ($env !== false && trim($env) !== '') {
            return trim($env);
        }
        $home = getenv('HOME');
        if ($home === false || $home === '') {
            $home = posix_getpwuid(posix_geteuid())['dir'] ?? null;
        }
        if ($home === null) {
            return null;
        }
        $path = rtrim($home, '/') . '/.goldlapel/dashboard-token';
        if (!is_file($path)) {
            return null;
        }
        $text = @file_get_contents($path);
        if ($text === false) {
            return null;
        }
        $text = trim($text);
        return $text === '' ? null : $text;
    }

    /**
     * Fetch (and cache) the canonical {tables, query_patterns} for a helper.
     *
     * @param array<string, array<string, mixed>> $cache passed by reference;
     *                                            caller supplies the per-instance cache.
     * @return array{tables: array<string, string>, query_patterns: array<string, string>}
     */
    public static function fetch(
        array &$cache,
        string $family,
        string $name,
        int $dashboardPort,
        ?string $dashboardToken
    ): array {
        $key = $family . ':' . $name;
        if (isset($cache[$key])) {
            return $cache[$key];
        }

        if ($dashboardToken === null || $dashboardToken === '') {
            throw new \RuntimeException(
                'No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let '
                . 'GoldLapel::start spawn the proxy (which provisions a token automatically).'
            );
        }
        if ($dashboardPort <= 0) {
            throw new \RuntimeException(
                "No dashboard port available. Gold Lapel's helper families "
                . "({$family}, ...) require the proxy's dashboard to be reachable."
            );
        }

        $url = "http://127.0.0.1:{$dashboardPort}/api/ddl/{$family}/create";
        $body = json_encode([
            'name' => $name,
            'schema_version' => self::supportedVersion($family),
        ]);

        [$status, $parsed, $raw] = self::postJson($url, $dashboardToken, $body);

        if ($status !== 200) {
            $error = (is_array($parsed) && isset($parsed['error'])) ? $parsed['error'] : 'unknown';
            $detail = (is_array($parsed) && isset($parsed['detail'])) ? $parsed['detail'] : $raw;
            if ($status === 409 && $error === 'version_mismatch') {
                throw new \RuntimeException(
                    "Gold Lapel schema version mismatch for {$family} '{$name}': {$detail}. "
                    . 'Upgrade the proxy or the wrapper so versions agree.'
                );
            }
            if ($status === 403) {
                throw new \RuntimeException(
                    'Gold Lapel dashboard rejected the DDL request (403). '
                    . 'The dashboard token is missing or incorrect — check '
                    . 'GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token.'
                );
            }
            throw new \RuntimeException(
                "Gold Lapel DDL API {$family}/{$name} failed with {$status} {$error}: {$detail}"
            );
        }

        $entry = [
            'tables' => $parsed['tables'] ?? [],
            'query_patterns' => $parsed['query_patterns'] ?? [],
        ];
        $cache[$key] = $entry;
        return $entry;
    }

    public static function invalidate(array &$cache): void
    {
        $cache = [];
    }

    /**
     * Translate the proxy's numbered $1/$2/... placeholders to PDO's `?`
     * positional syntax. The proxy only emits numbered placeholders of
     * form $N (1..99 here defensively; only $1..$9 in use today).
     */
    public static function toPdoPlaceholders(string $sql): string
    {
        return preg_replace('/\$\d+/', '?', $sql);
    }

    /**
     * @return array{0:int, 1:mixed, 2:string} [status, parsed_body_or_null, raw_text]
     */
    private static function postJson(string $url, string $token, string $body): array
    {
        // Use stream_context for portability — no curl extension dependency.
        $opts = [
            'http' => [
                'method' => 'POST',
                'header' => "Content-Type: application/json\r\n"
                         . "X-GL-Dashboard: {$token}\r\n",
                'content' => $body,
                'timeout' => 10.0,
                'ignore_errors' => true, // return body on 4xx/5xx instead of false
            ],
        ];
        $ctx = stream_context_create($opts);
        $raw = @file_get_contents($url, false, $ctx);
        if ($raw === false) {
            $lastErr = error_get_last();
            $msg = $lastErr['message'] ?? 'unknown';
            throw new \RuntimeException(
                "Gold Lapel dashboard not reachable at {$url}: {$msg}. "
                . 'Is `goldlapel` running? The dashboard port must be open '
                . 'for helper families (streams, docs, ...) to work.'
            );
        }
        $status = 0;
        if (!empty($http_response_header)) {
            // First line: "HTTP/1.1 200 OK"
            $m = null;
            if (preg_match('#^HTTP/\S+\s+(\d+)#', $http_response_header[0], $m)) {
                $status = (int) $m[1];
            }
        }
        $parsed = null;
        if ($raw !== '') {
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                $parsed = $decoded;
            }
        }
        return [$status, $parsed, $raw];
    }
}
