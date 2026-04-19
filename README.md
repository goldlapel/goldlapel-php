# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
composer require goldlapel/goldlapel
```

Requires PHP 8.1+ and the `pdo_pgsql` extension.

## Quick Start

```php
use GoldLapel\GoldLapel;

// Start the proxy — returns a ready-to-use GoldLapel instance.
$gl = GoldLapel::start('postgresql://user:pass@localhost:5432/mydb', [
    'port'      => 7932,
    'log_level' => 'info',
]);

// Use the proxy URL with PDO (or any Postgres driver).
$pdo = new PDO($gl->pdoDsn(), ...$gl->pdoCredentials());
$rows = $pdo->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC);

// Call Gold Lapel wrapper methods directly — they use the internal PDO.
$hits = $gl->search('articles', 'body', 'postgres tuning');
$gl->docInsert('events', ['type' => 'signup']);

// Stop the proxy when you're done.
$gl->stop();
```

`$gl->url()` returns the raw `postgresql://…` URL if you want to hand it to
another driver or library. `$gl->pdoDsn()` returns the same information in
PDO's `pgsql:host=…;port=…;dbname=…` format.

## Transactional scope with `using()`

All wrapper methods default to the factory-managed PDO. When you want them
to run on *your* PDO — for example to include them in a transaction — wrap
the work in `$gl->using()`:

```php
$pdo = new PDO($gl->pdoDsn(), ...$gl->pdoCredentials());
$pdo->beginTransaction();

$gl->using($pdo, function ($gl) {
    // Any wrapper method called inside this callback uses $pdo.
    $gl->docInsert('events', ['type' => 'order.created']);
    $gl->incr('counters', 'orders_today');
});

$pdo->commit();
```

The scope is unwound in `finally`, so it restores correctly even if the
callback throws. Nesting is supported.

## Per-call connection override

Every wrapper method accepts an optional `conn:` named argument — useful
for one-off calls that should run on a different connection without
changing the scope:

```php
$gl->docInsert('events', ['type' => 'x'], conn: $otherPdo);
```

Resolution order: explicit `conn:` argument > `using()` scope > internal PDO.

## Configuration

Pass configuration keys inline with options, or group them under
`'config'`:

```php
$gl = GoldLapel::start('postgresql://user:pass@localhost/mydb', [
    'port'             => 7932,
    'mode'             => 'waiter',
    'pool_size'        => 50,
    'disable_matviews' => true,
    'replica'          => ['postgresql://user:pass@replica1/mydb'],
]);
```

Keys use `snake_case` and map to CLI flags (`pool_size` → `--pool-size`).
Boolean keys are flags — `true` enables them. Array keys produce repeated
flags. Unknown keys throw `InvalidArgumentException`. List valid keys with
`GoldLapel::configKeys()`.

You can also set environment variables (`GOLDLAPEL_PROXY_PORT`,
`GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Laravel

Gold Lapel's Laravel integration is included in this package and
auto-registered via the `laravel/framework` package discovery. Add a
`goldlapel` block to any `pgsql` connection in `config/database.php`:

```php
'pgsql' => [
    'driver'   => 'pgsql',
    'host'     => env('DB_HOST', 'db.example.com'),
    'port'     => env('DB_PORT', '5432'),
    'database' => env('DB_DATABASE'),
    'username' => env('DB_USERNAME'),
    'password' => env('DB_PASSWORD'),

    'goldlapel' => [
        'enabled' => true,           // default true
        'port'    => 7932,           // default 7932
        'config'  => [
            'mode' => 'waiter',
        ],
    ],
],
```

On application boot, the service provider starts a Gold Lapel process for
each configured `pgsql` connection and rewrites the Laravel connection
config to point at `127.0.0.1:<proxy port>`. Your Eloquent / Query Builder
code works unchanged; reads go through the L1 cache where safe.

Disable per-connection with `'goldlapel' => ['enabled' => false]`.

## API reference

### `GoldLapel::start(string $url, array $options = []): GoldLapel`

Factory. Starts the proxy, opens an internal PDO, returns the instance.

Supported `$options` keys:

| Key           | Type    | Description                                                 |
|---------------|---------|-------------------------------------------------------------|
| `port`        | `int`   | Proxy port (default 7932)                                   |
| `log_level`   | string  | `trace`, `debug`, `info`, `warn`, `error`                   |
| `silent`      | `bool`  | Suppress the startup banner entirely (default `false` — banner prints to stderr) |
| `config`      | `array` | Map of proxy config keys (see `GoldLapel::configKeys()`)    |
| `extra_args`  | `array` | Raw extra CLI flags passed verbatim to the binary           |

Any top-level key that is not one of the reserved names above is treated
as a `config` entry and merged into `config`. So
`['port' => 7932, 'mode' => 'waiter']` is equivalent to
`['port' => 7932, 'config' => ['mode' => 'waiter']]`.

### `GoldLapel::startProxyOnly(string $url, array $options = []): string`

Low-level variant. Starts the proxy and returns just the URL string — no
internal PDO is opened. Useful when the framework manages PDO
construction (e.g. Laravel's service provider).

### Instance methods

- `$gl->url(): ?string` — `postgresql://…` URL for use with any Postgres driver
- `$gl->pdoDsn(): ?string` — `pgsql:host=…;port=…;dbname=…` for `new PDO(...)`
- `$gl->pdoCredentials(): array{?string,?string}` — `[user, pass]` tuple
- `$gl->using(\PDO $conn, callable $cb): mixed` — scoped override
- `$gl->cached(): CachedPDO` — internal PDO wrapped with L1 cache
- `$gl->stop(): void` — stop the proxy (idempotent, auto-called at shutdown)

Plus the full wrapper surface (~61 methods): `docInsert`, `docFind`,
`docAggregate`, `search`, `searchFuzzy`, `similar`, `publish`, `incr`,
`zadd`, `hset`, `streamAdd`, `percolate`, `analyze`, etc. Every method
accepts an optional `conn:` named argument.

#### `$gl->script(string $luaCode, mixed ...$args): ?string`

Run a Lua script via `pllua`. The trailing `...$args` are forwarded as the
script's text parameters.

> **No `conn:` override on `script()`.** The variadic `...$args` signature
> would swallow any trailing `\PDO` rather than treat it as the
> per-call connection. To run `script()` on a specific connection, wrap
> the call in `using()`:
>
> ```php
> $gl->using($myPdo, fn ($gl) => $gl->script($lua, 'arg1', 'arg2'));
> ```
>
> Or call the underlying static helper directly for one-off use:
>
> ```php
> GoldLapel\Utils::script($myPdo, $lua, 'arg1', 'arg2');
> ```

## Upgrading from 0.1.x

v0.2.0 is a breaking change — there is no compatibility shim.

**Old (0.1.x):**

```php
$gl = new GoldLapel('postgresql://user:pass@localhost/mydb');
$gl->startProxy();
$rows = $gl->search('articles', 'title', 'hello');
$gl->stopProxy();
```

**New (0.2.0):**

```php
$gl = GoldLapel::start('postgresql://user:pass@localhost/mydb');
$rows = $gl->search('articles', 'title', 'hello');
$gl->stop();
```

Changes:

- `new GoldLapel(...)` + `startProxy()` → `GoldLapel::start($url, $options)` factory
- `stopProxy()` → `stop()`
- `getUrl()` → `url()`
- Static `GoldLapel::start()` / `GoldLapel::startUrl()` / `GoldLapel::stop()` /
  `GoldLapel::proxyUrl()` / `GoldLapel::dashboardUrl()` are gone — the
  factory returns an instance; manage it explicitly. Use
  `GoldLapel::cleanupAll()` if you need a "stop everything" hook.
- Wrapper methods now accept an optional `conn:` named argument.
- `$gl->using($pdo, $cb)` is new — scopes wrapper calls to your own PDO.

## How it works

This package bundles the Gold Lapel Rust binary for your platform. When
you call `GoldLapel::start()`, it:

1. Locates the binary (bundled in `bin/`, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Opens a PDO to the proxy (with L1 native cache)
5. Returns the instance; cleans up automatically on shutdown

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
