# goldlapel/goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-php/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-php/actions/workflows/test.yml)

The PHP wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that caches query results and creates indexes automatically. Zero code changes beyond the connection string.

The wrapper finds the bundled proxy binary, starts and stops it with your app, translates options into proxy flags, and hands back a driver-ready connection. Caching happens in the proxy, so every client — this wrapper, `psql`, a cron job — shares one result cache that stays correct across connections. It also ships Postgres-backed helpers (search, documents, streams, counters, sorted sets, hashes, queues, geo, pub/sub) and a Laravel integration.

## Install

```bash
composer require goldlapel/goldlapel
```

Requires PHP 8.1+ and the `pdo_pgsql` extension.

## Quickstart

```php
use GoldLapel\GoldLapel;

// Spawn the proxy in front of your upstream DB
$gl = GoldLapel::start('postgresql://user:pass@localhost:5432/mydb');

// PDO can't take a postgresql:// URL directly — use the helpers
$pdo = new PDO($gl->pdoDsn(), ...$gl->pdoCredentials());
$rows = $pdo->query('SELECT * FROM users')->fetchAll(PDO::FETCH_ASSOC);

$gl->stop();  // (also cleaned up in __destruct)
```

Point PDO at `$gl->pdoDsn()` (with `$gl->pdoCredentials()`, since PDO doesn't accept `postgresql://` URLs directly). The PDO is a plain `\PDO` connected to the proxy; any other Postgres driver can use `$gl->url()`.

The proxy listens on two ports: the proxy itself (`proxy_port`, default 7932) and the dashboard (`dashboard_port`, default `proxy_port + 1`; `0` disables it). Start several proxies in one process and, unless you set `proxy_port`, each takes the next free pair — 7932/7933, then 7934/7935, and so on; `$gl->getProxyPort()` tells you which. The async factory (`GoldLapel\Amp\GoldLapel::start()`) takes the same options.

Document store and streams live under nested namespaces:

```php
$gl->documents->insert('orders', ['status' => 'pending']);
$pending = $gl->documents->find('orders', ['status' => 'pending']);
$gl->streams->add('clicks', ['url' => '/']);
```

Scoped transactional coordination via `$gl->using($pdo, $cb)`, Laravel auto-wiring, and native async via `GoldLapel\Amp\` are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `$gl->getDashboardUrl()`:

```php
echo $gl->getDashboardUrl();
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, Laravel integration, async (Amp), upgrading from v0.1, and production deployment: https://goldlapel.com/docs/php

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and the indexes it created from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
composer remove goldlapel/goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and the indexes it created go away.

## License

MIT. See `LICENSE`.
