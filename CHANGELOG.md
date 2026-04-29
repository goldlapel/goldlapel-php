# Changelog

## Unreleased

### Breaking changes

**Doc-store and stream methods moved under nested namespaces.** The flat
`$gl->doc*()` and `$gl->stream*()` methods are gone; document and stream
operations now live under `$gl->documents-><verb>()` and
`$gl->streams-><verb>()`. No backwards-compat aliases — search and replace
once.

The same change applies to the async surface (`GoldLapel\Amp\GoldLapel`):
verbs move from `$gl->docInsert(...)->await()` to
`$gl->documents->insert(...)->await()`.

Migration map (sync; the async surface mirrors it 1:1):

| Old (flat)                            | New (nested)                                |
| ------------------------------------- | ------------------------------------------- |
| `$gl->docInsert($name, $doc)`         | `$gl->documents->insert($name, $doc)`       |
| `$gl->docInsertMany($name, $docs)`    | `$gl->documents->insertMany($name, $docs)`  |
| `$gl->docFind($name, $filter)`        | `$gl->documents->find($name, $filter)`      |
| `$gl->docFindOne($name, $filter)`     | `$gl->documents->findOne($name, $filter)`   |
| `$gl->docFindCursor($name, ...)`      | `$gl->documents->findCursor($name, ...)`    |
| `$gl->docUpdate($name, $f, $u)`       | `$gl->documents->update($name, $f, $u)`     |
| `$gl->docUpdateOne($name, $f, $u)`    | `$gl->documents->updateOne($name, $f, $u)`  |
| `$gl->docDelete($name, $f)`           | `$gl->documents->delete($name, $f)`         |
| `$gl->docDeleteOne($name, $f)`        | `$gl->documents->deleteOne($name, $f)`      |
| `$gl->docFindOneAndUpdate(...)`       | `$gl->documents->findOneAndUpdate(...)`     |
| `$gl->docFindOneAndDelete(...)`       | `$gl->documents->findOneAndDelete(...)`     |
| `$gl->docDistinct($name, $field, $f)` | `$gl->documents->distinct($name, $field, $f)`|
| `$gl->docCount($name, $filter)`       | `$gl->documents->count($name, $filter)`     |
| `$gl->docCreateIndex($name, $keys)`   | `$gl->documents->createIndex($name, $keys)` |
| `$gl->docAggregate($name, $pipeline)` | `$gl->documents->aggregate($name, $pipeline)`|
| `$gl->docWatch($name, $cb)`           | `$gl->documents->watch($name, $cb)`         |
| `$gl->docUnwatch($name)`              | `$gl->documents->unwatch($name)`            |
| `$gl->docCreateTtlIndex($name, $n)`   | `$gl->documents->createTtlIndex($name, $n)` |
| `$gl->docRemoveTtlIndex($name)`       | `$gl->documents->removeTtlIndex($name)`     |
| `$gl->docCreateCapped($name, $max)`   | `$gl->documents->createCapped($name, $max)` |
| `$gl->docRemoveCap($name)`            | `$gl->documents->removeCap($name)`          |
| `$gl->docCreateCollection($name, ...)`| `$gl->documents->createCollection($name, ...)` |
| `$gl->streamAdd($name, $payload)`     | `$gl->streams->add($name, $payload)`        |
| `$gl->streamCreateGroup($name, $g)`   | `$gl->streams->createGroup($name, $g)`      |
| `$gl->streamRead($name, $g, $c, $n)`  | `$gl->streams->read($name, $g, $c, $n)`     |
| `$gl->streamAck($name, $g, $id)`      | `$gl->streams->ack($name, $g, $id)`         |
| `$gl->streamClaim($name, $g, $c, ...)`| `$gl->streams->claim($name, $g, $c, ...)`   |

The other namespaces (`$gl->search()`, `$gl->publish()` / `$gl->subscribe()`,
`$gl->incr()`, `$gl->zadd()`, `$gl->hset()`, `$gl->geoadd()`, ...) remain flat
and will migrate to nested form in subsequent releases (one namespace per
schema-to-core phase).

`$gl->documents` and `$gl->streams` are PHP 8.1 `readonly` public properties
— attempting to reassign them raises `Error`. Each holds a back-reference to
the parent client; per-instance state (license, dashboard token, PDO/Postgres
connection, DDL pattern cache) is read through that reference, never
duplicated.

**Doc-store DDL is now owned by the proxy.** The wrapper no longer emits
`CREATE TABLE _goldlapel.doc_<name>` SQL when a collection is first used.
Instead, `$gl->documents-><verb>()` calls `POST /api/ddl/doc_store/create`
against the proxy's dashboard port; the proxy runs the canonical DDL on its
management connection and returns the table reference + query patterns. The
wrapper caches `(tables, query_patterns)` per session — one HTTP round-trip
per `(family, name)` per session.

Canonical doc-store schema (v1) standardizes the column shape across every
Gold Lapel wrapper:

```
_id        UUID PRIMARY KEY DEFAULT gen_random_uuid()
data       JSONB NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Both timestamps are `NOT NULL` — kills the `created_at NOT NULL` /
`updated_at` drift surfaced in the v0.2 cross-wrapper compat audit. Any
wrapper (Python, JS, Ruby, Java, **PHP**, Go, .NET) writing to a doc-store
collection now produces the same table.

`$lookup.from` references in `$gl->documents->aggregate()` are resolved to
the foreign collection's canonical proxy table (e.g. `orders` →
`_goldlapel.doc_orders`) — each unique `from` collection triggers an
idempotent describe/create on the proxy and is cached for the session.

**Upgrade path for dev databases:** wipe and recreate. There is no
in-place migration. Pre-1.0, dev databases get rebuilt freely.

```bash
goldlapel clean   # drops _goldlapel.* tables
# ...drop/recreate your DB if needed...
```

If you have a v0.2-pre wrapper running against a v0.2-post proxy, the
wrapper's first `$gl->documents-><verb>()` call surfaces a clear
`version_mismatch` error pointing to this CHANGELOG.

**Direct `Utils::doc*` callers must supply `?array $patterns`.** The static
helpers in `GoldLapel\Utils` and `GoldLapel\Amp\Utils` no longer issue
`CREATE TABLE` themselves; they read the canonical table from
`$patterns['tables']['main']` and throw `RuntimeException` if patterns are
not supplied. Direct callers should migrate to the namespace verbs
(`$gl->documents->insert(...)`); the static helpers remain on the public
surface for advanced uses but require the patterns map.
