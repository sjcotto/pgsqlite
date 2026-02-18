# pgsqlite - 8-Hour Development Plan

Current state: 78 tests passing, basic PG wire protocol working with tokio-postgres.

---

## Hour 1: Parameterized Query Support (Critical Gap)

The extended query protocol accepts $1, $2 params but never substitutes them.

### Tasks
- [ ] Add tests: `test_parameterized_select`, `test_parameterized_insert`, `test_parameterized_update`, `test_parameterized_delete`
- [ ] Add tests: `test_null_parameter`, `test_multiple_parameters`, `test_parameter_type_coercion`
- [ ] Implement `$1`→`?1` parameter rewriting in the SQL translator
- [ ] Wire portal params from Bind into rusqlite's `execute` with actual bound values
- [ ] Add integration test: `tokio-postgres` `client.query("SELECT $1::int", &[&42i32])` round-trip
- [ ] Add integration test: `client.execute("INSERT INTO t VALUES ($1, $2)", &[&1i32, &"hello"])` round-trip

---

## Hour 2: Transaction State Tracking

Server always sends `ReadyForQuery { status: 'I' }` — never tracks BEGIN/COMMIT/ROLLBACK.

### Tasks
- [ ] Add tests: `test_begin_changes_status_to_T`, `test_commit_changes_status_to_I`, `test_rollback_changes_status_to_I`
- [ ] Add test: `test_error_in_transaction_changes_status_to_E`
- [ ] Add test: `test_sync_after_error_in_transaction_sends_rollback`
- [ ] Add `TransactionState` enum (Idle, InTransaction, Failed) to `ConnectionState`
- [ ] Update `handle_simple_query` and `handle_execute` to track state transitions
- [ ] Update `Sync` handler to send correct status byte
- [ ] Integration test: `tokio-postgres` transaction block (begin, insert, commit, verify)

---

## Hour 3: Column Type OID Inference

All columns come back as OID_TEXT (25). Clients/ORMs need accurate types.

### Tasks
- [ ] Add tests: `test_integer_column_returns_int4_oid`, `test_real_column_returns_float8_oid`, `test_text_column_returns_text_oid`, `test_blob_column_returns_bytea_oid`
- [ ] Add test: `test_boolean_column_type`, `test_null_column_type`
- [ ] Implement type inference from SQLite's `column_decltype()` — map declared types to PG OIDs
- [ ] Implement runtime value sniffing for columns without declared types (INTEGER/REAL/TEXT/BLOB/NULL)
- [ ] Add a `type_map` module with comprehensive SQLite-type-string → PG-OID mapping
- [ ] Integration test: tokio-postgres reads column types correctly

---

## Hour 4: Dynamic information_schema + pg_class from SQLite Metadata

Currently returns empty result sets. ORMs need real table/column info.

### Tasks
- [ ] Add tests: `test_information_schema_tables_reflects_created_tables`
- [ ] Add tests: `test_information_schema_columns_reflects_table_columns`
- [ ] Add tests: `test_pg_class_returns_table_entries`
- [ ] Add tests: `test_pg_attribute_returns_column_entries`
- [ ] Query `sqlite_master` to populate `information_schema.tables`
- [ ] Parse `CREATE TABLE` SQL from `sqlite_master.sql` to extract column definitions for `information_schema.columns`
- [ ] Map SQLite types back to PG type names for `data_type` column
- [ ] Populate `pg_class` (oid, relname, relnamespace=2200, relkind='r') from sqlite_master
- [ ] Populate `pg_attribute` (attrelid, attname, atttypid, attnum) from parsed column info
- [ ] Integration test: tokio-postgres reads table metadata

---

## Hour 5: More Function Emulation

Many PG functions are not yet translated.

### Tasks
- [ ] Add tests for each function translation:
  - `date_trunc('month', timestamp)` → SQLite strftime equivalent
  - `to_char(timestamp, 'YYYY-MM-DD')` → strftime
  - `extract(epoch FROM timestamp)` → strftime('%s', ...)
  - `coalesce(a, b)` → passes through (SQLite has it)
  - `nullif(a, b)` → passes through
  - `length(text)` → passes through
  - `lower(text)` / `upper(text)` → passes through
  - `trim(text)` / `ltrim(text)` / `rtrim(text)` → passes through
  - `substr(text, start, len)` → passes through
  - `replace(text, from, to)` → passes through
  - `abs(num)` / `round(num)` → passes through
  - `random()` → passes through (same name)
  - `current_timestamp` → datetime('now')
  - `current_date` → date('now')
  - `age(timestamp)` → custom function
  - `generate_series(start, stop)` → custom table-valued function
- [ ] Register additional SQLite custom functions for non-translatable ones
- [ ] Add backend tests exercising each translated function end-to-end

---

## Hour 6: RETURNING Clause + ON CONFLICT (UPSERT)

Critical for modern PG apps. SQLite 3.35+ supports RETURNING.

### Tasks
- [ ] Add tests: `test_insert_returning`, `test_update_returning`, `test_delete_returning`
- [ ] Add tests: `test_upsert_on_conflict_do_nothing`, `test_upsert_on_conflict_do_update`
- [ ] Detect RETURNING in INSERT/UPDATE/DELETE and handle as a query (return rows)
- [ ] Translate PG `ON CONFLICT (col) DO UPDATE SET col = EXCLUDED.col` syntax
- [ ] Handle `EXCLUDED` table reference → SQLite's `excluded` (same in modern SQLite)
- [ ] Integration test: insert with RETURNING via tokio-postgres

---

## Hour 7: COPY Protocol + Multi-Statement + LISTEN/NOTIFY Stubs

### Tasks — COPY
- [ ] Add tests: `test_copy_from_stdin_csv`, `test_copy_to_stdout`
- [ ] Implement COPY FROM STDIN handling (parse CopyData messages, insert rows)
- [ ] Implement COPY TO STDOUT (send CopyOutResponse + CopyData + CopyDone)

### Tasks — Multi-Statement
- [ ] Add test: `test_multi_statement_query` ("CREATE TABLE t(id INT); INSERT INTO t VALUES(1); SELECT * FROM t")
- [ ] Split semicolon-separated statements and execute each, sending results for each

### Tasks — LISTEN/NOTIFY Stubs
- [ ] Add test: `test_listen_accepted_silently`
- [ ] Add test: `test_notify_accepted_silently`
- [ ] Accept LISTEN/NOTIFY without error (no-op, log warning)

---

## Hour 8: Hardening, Edge Cases, and ORM Smoke Tests

### Tasks — Edge Cases
- [ ] Add test: `test_very_long_query` (100KB SQL string)
- [ ] Add test: `test_large_result_set` (10,000 rows)
- [ ] Add test: `test_binary_data_roundtrip` (BYTEA insert + select)
- [ ] Add test: `test_unicode_data_roundtrip` (emoji, CJK characters)
- [ ] Add test: `test_concurrent_connections` (5 parallel tokio-postgres clients)
- [ ] Add test: `test_connection_drop_mid_query` (client disconnects)
- [ ] Add test: `test_empty_table_select` (SELECT from table with no rows)
- [ ] Add test: `test_select_star_with_no_tables` (SELECT 1, SELECT 'hello')

### Tasks — ORM Compatibility Queries
- [ ] Add test: `test_show_transaction_isolation_level`
- [ ] Add test: `test_show_server_version`
- [ ] Add test: `test_set_search_path`
- [ ] Add test: `test_set_client_encoding`
- [ ] Add test: `test_select_pg_catalog_pg_database`
- [ ] Add test: `test_discard_all`
- [ ] Intercept and handle `SHOW` commands that ORMs send on connect
- [ ] Intercept `SET search_path` / `SET client_encoding` silently

### Tasks — Resilience
- [ ] Add test: `test_malformed_message_doesnt_crash`
- [ ] Add test: `test_oversized_message_rejected`
- [ ] Add bounds checking on message lengths
- [ ] Add timeout for idle connections

---

## Running Totals

| Metric | Current | After Plan |
|--------|---------|------------|
| Unit tests | 75 | ~200+ |
| Integration tests | 3 | ~20+ |
| PG functions emulated | 4 | ~20+ |
| Catalog tables emulated | 6 | 10+ |
| Features | Simple+Extended query, SSL reject, type mapping, catalog stubs | + params, transactions, RETURNING, UPSERT, COPY, type OIDs, dynamic catalog, ORM compat |
