# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
composer require goldlapel/goldlapel
```

## Quick Start

```php
use GoldLapel\GoldLapel;

// Start the proxy — returns a database connection with L1 cache built in
$conn = GoldLapel::start('postgresql://user:pass@localhost:5432/mydb');

// Use the connection directly — no PDO or driver setup needed
$result = $conn->query('SELECT * FROM users WHERE id = $1', [42]);
```

## API

### `GoldLapel::start(upstream, port?, config?, extraArgs?)`

Starts the Gold Lapel proxy and returns a database connection with L1 cache.

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
$conn = $proxy->startProxy();
// ...
$proxy->stopProxy();
```

## Configuration

Pass a config array to configure the proxy:

```php
use GoldLapel\GoldLapel;

$conn = GoldLapel::start('postgresql://user:pass@localhost/mydb', config: [
    'mode' => 'waiter',
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

You can also set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in `bin/`, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a database connection with L1 native cache built in
5. Cleans up automatically on shutdown

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
