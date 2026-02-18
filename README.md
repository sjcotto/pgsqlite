# pgsqlite

A PostgreSQL wire-protocol compatible server backed by SQLite. Use your existing PostgreSQL clients, ORMs, and tools with a single-file SQLite database.

```
pgsqlite mydata.db
```

That's it. Connect with `psql`, SQLAlchemy, Prisma, Django, TypeORM, or any PostgreSQL client.

## Why?

| | Real PostgreSQL | pgsqlite |
|---|---|---|
| **Setup** | Install, `initdb`, configure `pg_hba.conf`, create users | `cargo install pgsqlite` |
| **Database** | Cluster directory, WAL files, tablespaces | Single `.db` file |
| **Startup** | Seconds | Milliseconds |
| **Memory** | 100MB+ resident | ~5MB |
| **Binary** | ~100MB+ | ~5MB static binary |
| **Dependencies** | libpq, system libraries | None (SQLite is bundled) |

**Use cases:**

- **Dev/test** - No Docker, no `docker-compose`, no waiting for Postgres to boot
- **CI pipelines** - Instant database per test run, zero infrastructure
- **Prototyping** - Start with PG-compatible schema, swap to real Postgres when you're ready
- **Edge/embedded** - Single binary + single file for IoT, mobile backends, CLI tools
- **Unit tests** - In-memory `:memory:` database per test, sub-millisecond setup

## Quick Start

### From source

```bash
git clone https://github.com/sjcotto/pgsqlite.git
cd pgsqlite
cargo build --release
./target/release/pgsqlite mydata.db
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `PGSQLITE_ADDR` | `127.0.0.1:5432` | Listen address |
| `PGSQLITE_DB` | `pgsqlite.db` | SQLite database file (`:memory:` for in-memory) |

### Connect

```bash
# psql
psql -h 127.0.0.1 -p 5432 -U anyuser -d anydb

# Python
psql_url = "postgresql://user:pass@127.0.0.1:5432/mydb"

# Node.js
const connectionString = "postgres://user:pass@127.0.0.1:5432/mydb";
```

Any username, password, and database name are accepted.

## What Works

### Wire Protocol

Full PostgreSQL v3 wire protocol — both **Simple Query** and **Extended Query** (parameterized queries). This is what ORMs actually use.

| Feature | Status |
|---|---|
| Simple Query (`SELECT ...`) | Supported |
| Extended Query (Parse/Bind/Describe/Execute/Sync) | Supported |
| Parameterized queries (`$1`, `$2`, ...) | Supported |
| Transactions (BEGIN/COMMIT/ROLLBACK) | Supported |
| SAVEPOINT / RELEASE / ROLLBACK TO | Supported |
| COPY FROM STDIN | Supported |
| PREPARE / EXECUTE / DEALLOCATE (SQL-level) | Supported |
| EXPLAIN / EXPLAIN ANALYZE | Supported |
| SSL/TLS | Rejected cleanly |

### SQL Translation

PostgreSQL SQL is parsed into an AST and transformed to SQLite-compatible SQL.

**Data types:**

| PostgreSQL | SQLite |
|---|---|
| `VARCHAR(n)`, `CHAR(n)`, `TEXT` | `TEXT` |
| `BOOLEAN` | `TEXT` (`'true'`/`'false'`) |
| `INTEGER`, `BIGINT`, `SMALLINT` | `INTEGER` / `BIGINT` / `SMALLINT` |
| `REAL`, `DOUBLE PRECISION` | `REAL` |
| `NUMERIC`, `DECIMAL` | `REAL` |
| `SERIAL` / `BIGSERIAL` | `INTEGER PRIMARY KEY` (autoincrement) |
| `TIMESTAMP`, `TIMESTAMPTZ` | `TEXT` |
| `UUID` | `TEXT` |
| `JSON`, `JSONB` | `TEXT` |
| `BYTEA` | `BLOB` |

**Functions:**

| PostgreSQL | SQLite | Notes |
|---|---|---|
| `now()` | `datetime('now')` | |
| `gen_random_uuid()` | Custom function | Real UUID v4 |
| `string_agg(x, sep)` | `group_concat(x, sep)` | |
| `array_agg(x)` | `group_concat(x)` | |
| `date_trunc('month', ts)` | `strftime('%Y-%m-01 00:00:00', ts)` | All intervals supported |
| `to_char(ts, 'YYYY-MM-DD')` | `strftime('%Y-%m-%d', ts)` | Format tokens converted |
| `EXTRACT(YEAR FROM ts)` | `CAST(strftime('%Y', ts) AS INTEGER)` | |
| `position(sub IN str)` | `instr(str, sub)` | |
| `strpos(str, sub)` | `instr(str, sub)` | |
| `left(str, n)` / `right(str, n)` | `substr(...)` | |
| `char_length(x)` | `length(x)` | |
| `btrim(x)` | `trim(x)` | |
| `regexp_replace(s, p, r)` | `replace(s, p, r)` | Literal replacement only |
| `coalesce`, `lower`, `upper`, `substr`, `replace`, `abs`, `round`, `random`, `length` | Pass-through | SQLite native |

**SQL features:**

| Feature | How it works |
|---|---|
| `ILIKE` | Translated to `LOWER(expr) LIKE LOWER(pattern)` |
| `::type` casts | Converted to `CAST(expr AS type)` |
| `public.tablename` | Schema prefix stripped |
| `SERIAL` / `BIGSERIAL` | `INTEGER PRIMARY KEY` with autoincrement |
| `TRUNCATE TABLE` | Converted to `DELETE FROM` |
| `INSERT ... ON CONFLICT DO NOTHING` | Pass-through (SQLite native) |
| `INSERT ... ON CONFLICT DO UPDATE` | Pass-through (SQLite native) |
| `INSERT/UPDATE/DELETE ... RETURNING` | Pass-through (SQLite 3.35+) |
| `UPDATE ... FROM` | Pass-through (SQLite 3.33+) |
| `DELETE ... USING` | Rewritten to `DELETE ... WHERE EXISTS (...)` |
| `FILTER (WHERE ...)` on aggregates | Pass-through (SQLite 3.30+) |
| CTEs (`WITH ... AS`) | Pass-through (SQLite native) |
| Window functions | Pass-through (SQLite native) |
| Subqueries, JOINs, UNION | Pass-through |

### PostgreSQL Catalog Emulation

ORMs query system catalogs on startup. pgsqlite intercepts these and returns synthetic results.

| Catalog | Source |
|---|---|
| `pg_type` | Static (25 common types) |
| `pg_namespace` | Static (pg_catalog, public, information_schema) |
| `pg_class` | Dynamic from `sqlite_master` |
| `pg_attribute` | Dynamic from `PRAGMA table_info` |
| `pg_database` | Static (database "pgsqlite") |
| `pg_roles` / `pg_user` | Static (superuser "pgsqlite") |
| `pg_settings` | Static (10 common settings) |
| `pg_indexes` | Dynamic from `sqlite_master` |
| `pg_constraint` | Dynamic from `PRAGMA table_info` |
| `information_schema.tables` | Dynamic from `sqlite_master` |
| `information_schema.columns` | Dynamic from `PRAGMA table_info` |
| `version()` | `PostgreSQL 15.0.0 (pgsqlite)` |
| `current_database()` | `pgsqlite` |
| `current_schema()` | `public` |
| `current_user` / `session_user` | `pgsqlite` |

### Error Codes

SQLite errors are mapped to proper PostgreSQL SQLSTATE codes:

| Error | SQLSTATE | Code |
|---|---|---|
| Unique constraint violation | `unique_violation` | 23505 |
| Foreign key violation | `foreign_key_violation` | 23503 |
| NOT NULL violation | `not_null_violation` | 23502 |
| CHECK constraint violation | `check_violation` | 23514 |
| Table not found | `undefined_table` | 42P01 |
| Column not found | `undefined_column` | 42703 |
| Table already exists | `duplicate_table` | 42P07 |
| Syntax error | `syntax_error` | 42601 |
| Database locked | `serialization_failure` | 40001 |

### SHOW / SET Commands

All `SHOW` commands return appropriate values (`server_version`, `client_encoding`, `timezone`, etc.). All `SET` commands are silently accepted. `LISTEN`/`NOTIFY` are no-ops. `DISCARD ALL` and `RESET ALL` are handled.

## ORM Compatibility

Tested with query patterns from:

| ORM | Status | Key patterns tested |
|---|---|---|
| **SQLAlchemy** | Works | SERIAL, RETURNING, JOINs, `count()`, schema inspection |
| **Prisma** | Works | Quoted identifiers, `IF NOT EXISTS`, ON CONFLICT, aggregates |
| **Django** | Works | SHOW commands, ILIKE, `SET`, filter/count/exists patterns |
| **TypeORM** | Works | `current_schema()`, `current_database()`, findAndCount, aliases |
| **Drizzle** | Works | LIKE, GROUP BY + aggregates, LIMIT/OFFSET |

Connection pooling (via `deadpool-postgres`) works with concurrent queries.

## Architecture

```
┌──────────────┐     PG Wire Protocol     ┌──────────────────┐
│  PG Client   │ ◄──────────────────────► │    pgsqlite      │
│  (psql, ORM) │    TCP :5432             │                  │
└──────────────┘                          │  ┌────────────┐  │
                                          │  │  Protocol   │  │  Parse/Serialize
                                          │  │  Messages   │  │  PG wire messages
                                          │  └─────┬──────┘  │
                                          │        │         │
                                          │  ┌─────▼──────┐  │
                                          │  │  Catalog    │  │  Intercept system
                                          │  │  Emulation  │  │  catalog queries
                                          │  └─────┬──────┘  │
                                          │        │         │
                                          │  ┌─────▼──────┐  │
                                          │  │    SQL      │  │  PG AST → SQLite
                                          │  │ Translator  │  │  AST transform
                                          │  └─────┬──────┘  │
                                          │        │         │
                                          │  ┌─────▼──────┐  │
                                          │  │  SQLite     │  │  rusqlite + WAL
                                          │  │  Backend    │  │  mode + bundled
                                          │  └────────────┘  │
                                          └──────────────────┘
```

**Key design decisions:**

- **AST-level translation** via `sqlparser-rs` — not string hacking. Handles nested expressions, subqueries, CTEs correctly.
- **Bundled SQLite** — no system dependency, consistent behavior everywhere.
- **WAL mode** — enables concurrent readers while writing.
- **Foreign keys ON** — `PRAGMA foreign_keys=ON` by default.
- **Shared backend** — `Arc<Mutex<Connection>>` across all client connections.

## Limitations

- **No SSL/TLS** — connections are unencrypted (suitable for localhost/dev use)
- **No authentication** — all connections accepted
- **No arrays** — PostgreSQL array types are not supported
- **No COPY TO** — only `COPY FROM STDIN` is implemented
- **No LISTEN/NOTIFY delivery** — accepted but silently dropped
- **BOOLEAN is TEXT** — stored as `'true'`/`'false'` strings
- **NUMERIC precision** — mapped to REAL, loses exact decimal precision
- **Single writer** — writes serialize through a mutex (reads concurrent via WAL)
- **No cursors** — `max_rows` on Execute is ignored
- **Binary protocol** — all values sent as text format
- **Regex** — `regexp_replace` does literal string replacement, not regex

## Tests

```bash
cargo test          # 316 tests (224 unit + 92 integration)
cargo clippy        # Zero warnings
```

## License

MIT
