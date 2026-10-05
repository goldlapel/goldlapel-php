<?php

namespace GoldLapel\Laravel\Tests;

/**
 * A stand-in for the goldlapel binary, so the provider tests run the real
 * core — port allocation, reuse, option checks — without a Postgres. It
 * records each spawn's arguments, binds its proxy and dashboard ports
 * (refusing a busy one with the proxy's message), and idles.
 */
final class FakeProxy
{
    private static ?string $binary = null;
    private static ?string $log = null;
    private static string|false $originalBinary = false;

    private const SCRIPT = <<<'PY'
#!/usr/bin/env python3
import json, os, socket, sys, time

args = sys.argv[1:]
with open(os.environ['GL_FAKE_PROXY_LOG'], 'a') as f:
    f.write(json.dumps(args) + '\n')

def opt(flag, default=None):
    return args[args.index(flag) + 1] if flag in args else default

port = int(opt('--proxy-port'))
dashboard = int(opt('--dashboard-port', port + 1))
held = []
for p, what in ((port, 'proxy'), (dashboard, 'dashboard')):
    if p == 0:
        continue
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(('0.0.0.0', p))
    except OSError:
        print(f"I'm afraid port {p}, for the {what}, is already in use", file=sys.stderr)
        sys.exit(1)
    s.listen(5)
    held.append(s)
time.sleep(60)
PY;

    public static function install(): void
    {
        self::$binary = tempnam(sys_get_temp_dir(), 'gl_fake_proxy_');
        file_put_contents(self::$binary, self::SCRIPT);
        chmod(self::$binary, 0755);
        self::$log = tempnam(sys_get_temp_dir(), 'gl_fake_proxy_log_');

        self::$originalBinary = getenv('GOLDLAPEL_BINARY');
        putenv('GOLDLAPEL_BINARY=' . self::$binary);
        putenv('GL_FAKE_PROXY_LOG=' . self::$log);
    }

    public static function uninstall(): void
    {
        if (self::$originalBinary === false) {
            putenv('GOLDLAPEL_BINARY');
        } else {
            putenv('GOLDLAPEL_BINARY=' . self::$originalBinary);
        }
        putenv('GL_FAKE_PROXY_LOG');
        @unlink((string) self::$binary);
        @unlink((string) self::$log);
    }

    /**
     * The arguments of every spawn so far, oldest first.
     *
     * @return list<list<string>>
     */
    public static function spawns(): array
    {
        $lines = array_filter(explode("\n", (string) @file_get_contents((string) self::$log)));
        return array_values(array_map(fn ($line) => json_decode($line, true), $lines));
    }

    /** The value following $flag in $args, or null. */
    public static function option(array $args, string $flag): ?string
    {
        $i = array_search($flag, $args, true);
        return $i === false ? null : ($args[$i + 1] ?? null);
    }
}
