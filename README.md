# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
composer require goldlapel/goldlapel
```

## Quick Start

```php
use GoldLapel\GoldLapel;

// Start the proxy — returns a connection string pointing at Gold Lapel
$url = GoldLapel::start('postgresql://user:pass@localhost:5432/mydb');

// Use with any Postgres driver or ORM
// Laravel, Doctrine, Eloquent — anything that accepts a connection URL
```

Gold Lapel is driver-agnostic. `start()` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

## API

### `GoldLapel::start(upstream, port?, config?, extraArgs?)`

Starts the Gold Lapel proxy and returns the proxy connection string.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `port` — proxy port (default: 7932)
- `config` — associative array of configuration keys (see [Configuration](#configuration))
- `extraArgs` — additional CLI flags passed to the binary (e.g. `['--threshold-impact', '5000']`)

### `GoldLapel::stop()`

Stops the proxy. Also called automatically via `register_shutdown_function`.

### `GoldLapel::proxyUrl()`

Returns the current proxy URL, or `null` if not running.

### `GoldLapel::dashboardUrl()`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if not running. The dashboard port defaults to 7933 and can be changed via `config: ['dashboard_port' => 8080]` or disabled with `0`.

### `new GoldLapel(upstream, port?, config?, extraArgs?)`

Instance API for managing multiple proxies:

```php
use GoldLapel\GoldLapel;

$proxy = new GoldLapel('postgresql://user:pass@localhost:5432/mydb', 7932);
$url = $proxy->startProxy();
// ...
$proxy->stopProxy();
```

## Configuration

Pass a config array to configure the proxy:

```php
use GoldLapel\GoldLapel;

$url = GoldLapel::start('postgresql://user:pass@localhost/mydb', config: [
    'mode' => 'butler',
    'pool_size' => 50,
    'disable_matviews' => true,
    'replica' => ['postgresql://user:pass@replica1/mydb'],
]);
```

Keys use `snake_case` and map to CLI flags (`pool_size` → `--pool-size`). Boolean keys are flags — `true` enables them. Array keys produce repeated flags.

Unknown keys throw `InvalidArgumentException`. To see all valid keys:

```php
GoldLapel::configKeys()
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Driver Examples

### PDO

PDO doesn't accept `postgresql://` URLs directly — it needs a DSN string. Parse the proxy URL or build the DSN yourself:

```php
$url = GoldLapel::start('postgresql://user:pass@localhost:5432/mydb');

// Parse the proxy URL into a PDO DSN
$parts = parse_url($url);
$dsn = sprintf('pgsql:host=%s;port=%d;dbname=%s',
    $parts['host'], $parts['port'], ltrim($parts['path'], '/'));
$pdo = new PDO($dsn, $parts['user'], $parts['pass']);
```

> **Note:** Laravel, Doctrine, and most ORMs accept `postgresql://` URLs natively — no parsing needed.

### Laravel

Set the proxy URL in your database config or `.env`:

```php
// bootstrap/app.php or a service provider
$url = GoldLapel::start(config('database.connections.pgsql.url'));

config(['database.connections.pgsql.url' => $url]);
```

### Doctrine

```php
$url = GoldLapel::start('postgresql://user:pass@localhost:5432/mydb');

$conn = DriverManager::getConnection(['url' => $url]);
```

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in `bin/`, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy
5. Cleans up automatically on shutdown

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
