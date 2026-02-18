# pgsqlite - 8-Hour Development Plan (Phase 2)

Current state: 316 tests passing (224 unit + 92 integration), zero clippy warnings.
ALL 8 HOURS COMPLETE.

---

## Hour 1: ILIKE, Schema-Qualified Names, Type Casting (::)

- [x] Translate ILIKE → LIKE with COLLATE NOCASE or upper() wrapping
- [x] Strip schema qualifiers (public.tablename → tablename)
- [x] Handle PG double-colon cast (::text, ::integer) in more contexts
- [x] Add tests for each

---

## Hour 2: SERIAL/BIGSERIAL Autoincrement + DEFAULT Values

- [x] SERIAL → INTEGER PRIMARY KEY AUTOINCREMENT
- [x] DEFAULT gen_random_uuid(), DEFAULT now(), DEFAULT current_timestamp
- [x] INSERT with DEFAULT keyword
- [x] TRUNCATE TABLE → DELETE FROM
- [x] Add tests for each

---

## Hour 3: More PG Catalog Tables

- [x] pg_database (current database info)
- [x] pg_roles / pg_user (current user info)
- [x] pg_settings (server settings)
- [x] pg_indexes (index info from sqlite_master)
- [x] pg_constraint (constraints from table_info)
- [x] Add tests for each (14 new unit tests, 5 new integration tests)

---

## Hour 4: PREPARE/EXECUTE/DEALLOCATE + SAVEPOINT

- [x] Handle SQL-level PREPARE name AS query
- [x] Handle EXECUTE name(params)
- [x] Handle DEALLOCATE name
- [x] Handle SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT
- [x] Add tests (8 unit tests, 3 integration tests)

---

## Hour 5: EXPLAIN Support + More Function Emulation

- [x] EXPLAIN → SQLite's EXPLAIN QUERY PLAN
- [x] EXPLAIN ANALYZE → execute + show plan
- [x] regexp_replace() → replace() (literal string replacement)
- [x] position() → instr()
- [x] strpos() → instr()
- [x] left() / right() → substr equivalents
- [x] char_length() / character_length() → length()
- [x] btrim() → trim()
- [x] Add tests for each (9 unit tests, 5 integration tests)

---

## Hour 6: Error Code Improvements + Robustness

- [x] Map SQLite error codes to PG SQLSTATE codes via sqlite_error_to_sqlstate()
- [x] Unique violation → 23505
- [x] Foreign key violation → 23503
- [x] NOT NULL violation → 23502
- [x] CHECK constraint violation → 23514
- [x] Table not found → 42P01
- [x] Column not found → 42703
- [x] Table already exists → 42P07
- [x] Syntax error → 42601
- [x] Parse error → 42601
- [x] Database locked → 40001
- [x] Type mismatch → 22P02
- [x] Add tests (11 unit tests, 6 integration tests)

---

## Hour 7: UPDATE/DELETE with JOINs + Advanced SQL

- [x] UPDATE ... FROM (PG multi-table update, with FROM clause expression transforms)
- [x] DELETE ... USING → EXISTS subquery rewrite
- [x] INSERT ... ON CONFLICT with complex DO UPDATE expressions
- [x] INSERT/UPDATE/DELETE RETURNING clause support
- [x] Aggregate FILTER (WHERE ...) clause (SQLite 3.30+ native)
- [x] Add tests (6 unit + 5 integration)

---

## Hour 8: ORM Smoke Tests + Polish

- [x] SQLAlchemy-style query patterns (SERIAL, RETURNING, JOINs, count)
- [x] Prisma-style query patterns (quoted identifiers, IF NOT EXISTS, ON CONFLICT, aggregates)
- [x] Django ORM-style query patterns (SHOW commands, ILIKE, SET, filter/count/exists)
- [x] TypeORM-style query patterns (current_schema, current_database, aliases, findAndCount)
- [x] Drizzle ORM-style query patterns (LIKE, GROUP BY + aggregate, LIMIT)
- [x] Connection pool concurrency test (deadpool + 10 concurrent queries)
- [x] Common startup query test (SHOW commands, SET, current_user, catalog queries)
- [x] Complex query patterns (CTE, subquery+IN, CASE WHEN, COALESCE, DISTINCT, LIMIT+OFFSET)
- [x] Fixed DEFAULT CURRENT_TIMESTAMP for CREATE TABLE (don't transform to datetime('now'))
- [x] Added current_user/session_user catalog interception
- [x] Added SHOW server_version_num
- [x] Final clippy clean - zero warnings

---

## Running Totals

| Metric | Start | Target | Final |
|--------|-------|--------|-------|
| Unit tests | 160 | ~250+ | 224 |
| Integration tests | 54 | ~100+ | 92 |
| Total tests | 214 | ~350+ | 316 |
