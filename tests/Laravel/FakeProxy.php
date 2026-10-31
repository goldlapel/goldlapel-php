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
import fcntl, json, os, socket, sys, time

args = sys.argv[1:]
with open(os.environ['GL_FAKE_PROXY_LOG'], 'a') as f:
    f.write(json.dumps(args) + '\n')

def opt(flag, default=None):
    return args[args.index(flag) + 1] if flag in args else default

# A shared instance, as the proxy runs one: --detach returns once the proxy
# it starts serves (or with its status); the proxy owns <prefix>.lock and
# says where it is in <prefix>.json.
prefix = opt('--instance')
if '--detach' in args:
    child = os.fork()
    if child:
        while True:
            try:
                if json.load(open(prefix + '.json'))['pid'] == child:
                    sys.exit(0)
            except (OSError, ValueError, KeyError):
                pass
            done, status = os.waitpid(child, os.WNOHANG)
            if done:
                sys.exit(os.waitstatus_to_exitcode(status) or 1)
            time.sleep(0.02)
    os.setsid()
    with open(os.environ['GL_FAKE_PROXY_LOG'] + '.pids', 'a') as f:
        f.write(f'{os.getpid()}\n')
if prefix:
    lock = open(prefix + '.lock', 'a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit(3)

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
if prefix:
    with open(prefix + '.json.tmp', 'w') as f:
        json.dump({'pid': os.getpid(), 'proxy_port': port, 'dashboard_port': dashboard,
                   'dashboard_token': os.environ.get('GOLDLAPEL_DASHBOARD_TOKEN')}, f)
    os.rename(prefix + '.json.tmp', prefix + '.json')
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
        // Shared ones outlive the provider: they're stopped here
        foreach (array_filter(explode("\n", (string) @file_get_contents(self::$log . '.pids'))) as $pid) {
            posix_kill((int) $pid, SIGTERM);
        }
        @unlink(self::$log . '.pids');
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
