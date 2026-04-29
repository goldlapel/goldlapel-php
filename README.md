# goldlapel/goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-php/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-php/actions/workflows/test.yml)

The PHP wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Point PDO at `$gl->pdoDsn()` (with `$gl->pdoCredentials()`, since PDO doesn't accept `postgresql://` URLs directly). Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

Document store and streams live under nested namespaces:

```php
$gl->documents->insert('orders', ['status' => 'pending']);
$pending = $gl->documents->find('orders', ['status' => 'pending']);
$gl->streams->add('clicks', ['url' => '/']);
```

Scoped transactional coordination via `$gl->using($pdo, $cb)`, Laravel auto-wiring, and native async via `GoldLapel\Amp\` are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `$gl->dashboardUrl()`:

```php
echo $gl->dashboardUrl();
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, Laravel integration, async (Amp), upgrading from v0.1, and production deployment: https://goldlapel.com/docs/php

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and cached matviews from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
composer remove goldlapel/goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and cached matviews go away.

## License

MIT. See `LICENSE`.
