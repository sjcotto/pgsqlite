use rusqlite::Connection;
use std::sync::{Arc, Mutex};
use crate::sql::translator::Translator;

pub struct SqliteBackend {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Option<String>>>,
    pub rows_affected: u64,
    pub command_tag: String,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: i32,
}

// PostgreSQL type OIDs
pub const OID_BOOL: i32 = 16;
pub const OID_BYTEA: i32 = 17;
pub const OID_INT2: i32 = 21;
pub const OID_INT4: i32 = 23;
pub const OID_INT8: i32 = 20;
pub const OID_FLOAT4: i32 = 700;
pub const OID_FLOAT8: i32 = 701;
pub const OID_TEXT: i32 = 25;
pub const OID_VARCHAR: i32 = 1043;
pub const OID_JSON: i32 = 114;
pub const OID_JSONB: i32 = 3802;
pub const OID_UUID: i32 = 2950;
pub const OID_TIMESTAMP: i32 = 1114;
pub const OID_TIMESTAMPTZ: i32 = 1184;
pub const OID_DATE: i32 = 1082;
pub const OID_NUMERIC: i32 = 1700;

/// Map a SQLite declared column type string to a PG type OID
fn decltype_to_oid(decltype: &str) -> i32 {
    let upper = decltype.to_uppercase();
    // Strip parenthesized lengths like VARCHAR(255) -> VARCHAR
    let base = if let Some(idx) = upper.find('(') {
        upper[..idx].trim()
    } else {
        upper.trim()
    };
    match base {
        "INTEGER" | "INT" | "INT4" => OID_INT4,
        "BIGINT" | "INT8" => OID_INT8,
        "SMALLINT" | "INT2" => OID_INT2,
        "REAL" | "FLOAT" | "FLOAT4" => OID_FLOAT4,
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => OID_FLOAT8,
        "NUMERIC" | "DECIMAL" => OID_NUMERIC,
        "TEXT" | "CLOB" => OID_TEXT,
        "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" => OID_VARCHAR,
        "BLOB" => OID_BYTEA,
        "BOOLEAN" | "BOOL" => OID_BOOL,
        "JSON" => OID_JSON,
        "JSONB" => OID_JSONB,
        "UUID" => OID_UUID,
        "TIMESTAMP" => OID_TIMESTAMP,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => OID_TIMESTAMPTZ,
        "DATE" => OID_DATE,
        _ => OID_TEXT,
    }
}

/// Infer PG OID from a SQLite runtime value type (used for columns without declared types)
#[allow(dead_code)]
fn value_type_to_oid(value: &rusqlite::types::ValueRef) -> i32 {
    match value {
        rusqlite::types::ValueRef::Null => OID_TEXT,
        rusqlite::types::ValueRef::Integer(_) => OID_INT8,
        rusqlite::types::ValueRef::Real(_) => OID_FLOAT8,
        rusqlite::types::ValueRef::Text(_) => OID_TEXT,
        rusqlite::types::ValueRef::Blob(_) => OID_BYTEA,
    }
}

impl SqliteBackend {
    pub fn new(path: &str) -> Result<Self, String> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(path)
        }.map_err(|e| format!("Failed to open SQLite: {}", e))?;

        // Enable WAL mode for single-writer/multi-reader
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| format!("Failed to set pragmas: {}", e))?;

        let backend = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        backend.register_functions()?;
        Ok(backend)
    }

    fn register_functions(&self) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();

        // gen_random_uuid() → pgsqlite_uuid_v4()
        conn.create_scalar_function("pgsqlite_uuid_v4", 0, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, |_ctx| {
            let uuid = uuid::Uuid::new_v4();
            Ok(uuid.to_string())
        }).map_err(|e| format!("Failed to register uuid function: {}", e))?;

        // current_setting() - stub for PG compat
        conn.create_scalar_function("current_setting", 1, rusqlite::functions::FunctionFlags::SQLITE_UTF8, |ctx| {
            let setting: String = ctx.get(0)?;
            match setting.as_str() {
                "server_version" => Ok("15.0".to_string()),
                _ => Ok("".to_string()),
            }
        }).map_err(|e| format!("Failed to register current_setting: {}", e))?;

        Ok(())
    }

    pub fn execute(&self, sql: &str) -> Result<QueryResult, String> {
        let translated = Translator::translate(sql)?;
        if translated.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "EMPTY".to_string(),
            });
        }

        let conn = self.conn.lock().unwrap();
        let trimmed = translated.trim();

        // Determine command type
        let upper = trimmed.to_uppercase();
        let has_returning = Self::has_returning_clause(&upper);
        if upper.starts_with("SELECT") || upper.starts_with("WITH") {
            Self::execute_query(&conn, trimmed)
        } else if upper.starts_with("INSERT") {
            if has_returning {
                Self::execute_returning(&conn, trimmed, "INSERT")
            } else {
                Self::execute_modify(&conn, trimmed, "INSERT")
            }
        } else if upper.starts_with("UPDATE") {
            if has_returning {
                Self::execute_returning(&conn, trimmed, "UPDATE")
            } else {
                Self::execute_modify(&conn, trimmed, "UPDATE")
            }
        } else if upper.starts_with("DELETE") {
            if has_returning {
                Self::execute_returning(&conn, trimmed, "DELETE")
            } else {
                Self::execute_modify(&conn, trimmed, "DELETE")
            }
        } else if upper.starts_with("CREATE") {
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "CREATE TABLE".to_string(),
            })
        } else if upper.starts_with("DROP") {
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "DROP TABLE".to_string(),
            })
        } else if upper.starts_with("TRUNCATE") {
            // TRUNCATE TABLE x → DELETE FROM x
            let table_name = upper
                .trim_start_matches("TRUNCATE")
                .trim()
                .trim_start_matches("TABLE")
                .split_whitespace()
                .next()
                .unwrap_or("");
            let delete_sql = format!("DELETE FROM {}", table_name);
            let affected = conn.execute(&delete_sql, [])
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: affected as u64,
                command_tag: "TRUNCATE TABLE".to_string(),
            })
        } else if upper.starts_with("SAVEPOINT ") {
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "SAVEPOINT".to_string(),
            })
        } else if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "RELEASE".to_string(),
            })
        } else if upper.starts_with("ROLLBACK TO SAVEPOINT ") || upper.starts_with("ROLLBACK TO ") {
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "ROLLBACK".to_string(),
            })
        } else if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION")
                || upper.starts_with("COMMIT") || upper.starts_with("END")
                || upper.starts_with("ROLLBACK") || upper.starts_with("SET")
                || upper.starts_with("DISCARD") || upper.starts_with("RESET") {
            // Transaction control and SET commands
            if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
                conn.execute_batch("BEGIN")
                    .map_err(|e| format!("Execute error: {}", e))?;
            } else if upper.starts_with("COMMIT") || upper.starts_with("END") {
                conn.execute_batch("COMMIT")
                    .map_err(|e| format!("Execute error: {}", e))?;
            } else if upper.starts_with("ROLLBACK") {
                conn.execute_batch("ROLLBACK")
                    .map_err(|e| format!("Execute error: {}", e))?;
            }
            // SET/DISCARD/RESET are silently accepted
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: upper.split_whitespace().next().unwrap_or("OK").to_string(),
            })
        } else {
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "OK".to_string(),
            })
        }
    }

    /// Execute a query with bound parameters (for extended query protocol)
    pub fn execute_with_params(&self, sql: &str, params: &[Option<Vec<u8>>]) -> Result<QueryResult, String> {
        let translated = Translator::translate(sql)?;
        if translated.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "EMPTY".to_string(),
            });
        }

        let conn = self.conn.lock().unwrap();
        let trimmed = translated.trim();

        // Convert params to rusqlite values
        let rusqlite_params: Vec<Box<dyn rusqlite::types::ToSql>> = params.iter().map(|p| {
            match p {
                Some(bytes) => {
                    let s = String::from_utf8_lossy(bytes).to_string();
                    Box::new(s) as Box<dyn rusqlite::types::ToSql>
                }
                None => Box::new(rusqlite::types::Null) as Box<dyn rusqlite::types::ToSql>,
            }
        }).collect();

        let param_refs: Vec<&dyn rusqlite::types::ToSql> = rusqlite_params.iter()
            .map(|p| p.as_ref())
            .collect();

        let upper = trimmed.to_uppercase();
        let has_returning = upper.contains(" RETURNING ");
        if upper.starts_with("SELECT") || upper.starts_with("WITH") {
            Self::execute_query_with_params(&conn, trimmed, &param_refs)
        } else if upper.starts_with("INSERT") {
            if has_returning {
                Self::execute_returning_with_params(&conn, trimmed, "INSERT", &param_refs)
            } else {
                Self::execute_modify_with_params(&conn, trimmed, "INSERT", &param_refs)
            }
        } else if upper.starts_with("UPDATE") {
            if has_returning {
                Self::execute_returning_with_params(&conn, trimmed, "UPDATE", &param_refs)
            } else {
                Self::execute_modify_with_params(&conn, trimmed, "UPDATE", &param_refs)
            }
        } else if upper.starts_with("DELETE") {
            if has_returning {
                Self::execute_returning_with_params(&conn, trimmed, "DELETE", &param_refs)
            } else {
                Self::execute_modify_with_params(&conn, trimmed, "DELETE", &param_refs)
            }
        } else {
            // For DDL and other commands, execute without params
            conn.execute_batch(trimmed)
                .map_err(|e| format!("Execute error: {}", e))?;
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                command_tag: "OK".to_string(),
            })
        }
    }

    /// Query sqlite_master for table names
    pub fn query_sqlite_master_tables(&self) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).unwrap_or_else(|_| panic!("Failed to query sqlite_master"));
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap_or_else(|_| panic!("Failed to read sqlite_master"))
            .filter_map(|r| r.ok())
            .collect()
    }

    /// Query sqlite_master for column info: (table_name, column_name, type, nullable, ordinal_position)
    pub fn query_sqlite_master_columns(&self) -> Vec<(String, String, String, bool, i32)> {
        let conn = self.conn.lock().unwrap();
        let tables: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect()
        };

        let mut result = Vec::new();
        for table in tables {
            let pragma_sql = format!("PRAGMA table_info(\"{}\")", table);
            let mut stmt = conn.prepare(&pragma_sql).unwrap();
            let columns: Vec<(String, String, bool, i32)> = stmt.query_map([], |row| {
                let cid: i32 = row.get(0)?;          // cid (column index)
                let name: String = row.get(1)?;      // name
                let col_type: String = row.get(2)?;   // type
                let notnull: i32 = row.get(3)?;       // notnull
                Ok((name, col_type, notnull == 0, cid + 1)) // ordinal_position is 1-based
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

            for (col_name, col_type, nullable, ordinal) in columns {
                result.push((table.clone(), col_name, col_type, nullable, ordinal));
            }
        }
        result
    }

    /// Query sqlite_master for index info: (index_name, table_name, sql)
    pub fn query_sqlite_master_indexes(&self) -> Vec<(String, String, Option<String>)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).unwrap_or_else(|_| panic!("Failed to query sqlite_master for indexes"));
        stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let tbl_name: String = row.get(1)?;
            let sql: Option<String> = row.get(2)?;
            Ok((name, tbl_name, sql))
        })
        .unwrap_or_else(|_| panic!("Failed to read sqlite_master indexes"))
        .filter_map(|r| r.ok())
        .collect()
    }

    /// Query table constraints: returns (table_name, column_name, constraint_type)
    /// constraint_type is "PRIMARY KEY", "NOT NULL", or "UNIQUE"
    pub fn query_constraints(&self) -> Vec<(String, String, String)> {
        let conn = self.conn.lock().unwrap();
        let tables: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect()
        };

        let mut result = Vec::new();
        for table in tables {
            let pragma_sql = format!("PRAGMA table_info(\"{}\")", table);
            let mut stmt = conn.prepare(&pragma_sql).unwrap();
            let columns: Vec<(String, bool, bool)> = stmt.query_map([], |row| {
                let name: String = row.get(1)?;      // name
                let notnull: i32 = row.get(3)?;       // notnull
                let pk: i32 = row.get(5)?;            // pk
                Ok((name, notnull != 0, pk != 0))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

            for (col_name, notnull, pk) in columns {
                if pk {
                    result.push((table.clone(), col_name.clone(), "PRIMARY KEY".to_string()));
                }
                if notnull {
                    result.push((table.clone(), col_name.clone(), "NOT NULL".to_string()));
                }
            }

            // Check for unique indexes on this table
            let idx_sql = format!(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{}' AND sql LIKE '%UNIQUE%'",
                table
            );
            if let Ok(mut idx_stmt) = conn.prepare(&idx_sql) {
                let unique_indexes: Vec<String> = idx_stmt.query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                for idx_name in unique_indexes {
                    let info_sql = format!("PRAGMA index_info(\"{}\")", idx_name);
                    if let Ok(mut info_stmt) = conn.prepare(&info_sql) {
                        let cols: Vec<String> = info_stmt.query_map([], |row| row.get::<_, String>(2))
                            .unwrap()
                            .filter_map(|r| r.ok())
                            .collect();
                        for col in cols {
                            result.push((table.clone(), col, "UNIQUE".to_string()));
                        }
                    }
                }
            }
        }
        result
    }

    /// Execute raw SQL without translation (for internal use like EXPLAIN QUERY PLAN)
    pub fn execute_raw(&self, sql: &str) -> Result<QueryResult, String> {
        let conn = self.conn.lock().unwrap();
        let trimmed = sql.trim();
        Self::execute_query(&conn, trimmed)
    }

    /// Describe a query without executing it - returns column info
    pub fn describe_query(&self, sql: &str) -> Result<Vec<ColumnInfo>, String> {
        let translated = Translator::translate(sql)?;
        if translated.is_empty() {
            return Ok(vec![]);
        }
        let conn = self.conn.lock().unwrap();
        let stmt = conn.prepare(&translated)
            .map_err(|e| format!("Prepare error: {}", e))?;
        let column_count = stmt.column_count();
        let columns: Vec<ColumnInfo> = (0..column_count)
            .map(|i| {
                let name = stmt.column_name(i).unwrap_or("?").to_string();
                let type_oid = stmt.columns()[i].decl_type()
                    .map(decltype_to_oid)
                    .unwrap_or(OID_TEXT);
                ColumnInfo { name, type_oid }
            })
            .collect();
        Ok(columns)
    }

    /// Execute a COPY FROM STDIN operation - insert multiple rows from CSV data
    pub fn copy_from_stdin(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<String>],
        _delimiter: char,
    ) -> Result<u64, String> {
        let conn = self.conn.lock().unwrap();

        let col_clause = if columns.is_empty() {
            String::new()
        } else {
            format!(" ({})", columns.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", "))
        };

        let placeholders = if columns.is_empty() {
            // Need to figure out column count from table
            let mut table_stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))
                .map_err(|e| format!("PRAGMA error: {}", e))?;
            let col_count = table_stmt.query_map([], |_| Ok(()))
                .map_err(|e| format!("PRAGMA query error: {}", e))?
                .count();
            (0..col_count).map(|_| "?").collect::<Vec<_>>().join(", ")
        } else {
            (0..columns.len()).map(|_| "?").collect::<Vec<_>>().join(", ")
        };

        let insert_sql = format!("INSERT INTO \"{}\"{} VALUES ({})", table, col_clause, placeholders);

        let tx = conn.execute("BEGIN", [])
            .map_err(|e| format!("Begin error: {}", e));
        if tx.is_err() {
            // May already be in a transaction
        }

        let mut count = 0u64;
        for row in rows {
            let params: Vec<&dyn rusqlite::types::ToSql> = row.iter()
                .map(|v| v as &dyn rusqlite::types::ToSql)
                .collect();
            conn.execute(&insert_sql, params.as_slice())
                .map_err(|e| {
                    let _ = conn.execute("ROLLBACK", []);
                    format!("COPY insert error: {}", e)
                })?;
            count += 1;
        }

        let _ = conn.execute("COMMIT", []);
        Ok(count)
    }

    fn execute_query_with_params(
        conn: &Connection,
        sql: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<QueryResult, String> {
        let mut stmt = conn.prepare(sql)
            .map_err(|e| format!("Prepare error: {}", e))?;

        let column_count = stmt.column_count();
        let columns: Vec<ColumnInfo> = (0..column_count)
            .map(|i| {
                let name = stmt.column_name(i).unwrap_or("?").to_string();
                let type_oid = stmt.columns()[i].decl_type()
                    .map(decltype_to_oid)
                    .unwrap_or(OID_TEXT);
                ColumnInfo { name, type_oid }
            })
            .collect();

        let rows: Vec<Vec<Option<String>>> = stmt.query_map(
            rusqlite::params_from_iter(params.iter()),
            |row| {
                let mut vals = Vec::new();
                for i in 0..column_count {
                    let val: Option<String> = match row.get_ref(i) {
                        Ok(rusqlite::types::ValueRef::Null) => None,
                        Ok(rusqlite::types::ValueRef::Integer(n)) => Some(n.to_string()),
                        Ok(rusqlite::types::ValueRef::Real(f)) => Some(f.to_string()),
                        Ok(rusqlite::types::ValueRef::Text(s)) => {
                            Some(String::from_utf8_lossy(s).to_string())
                        }
                        Ok(rusqlite::types::ValueRef::Blob(b)) => {
                            Some(format!("\\x{}", hex::encode(b)))
                        }
                        Err(_) => None,
                    };
                    vals.push(val);
                }
                Ok(vals)
            },
        )
        .map_err(|e| format!("Query error: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

        let row_count = rows.len();
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: row_count as u64,
            command_tag: format!("SELECT {}", row_count),
        })
    }

    fn execute_modify_with_params(
        conn: &Connection,
        sql: &str,
        cmd: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<QueryResult, String> {
        let affected = conn.execute(sql, rusqlite::params_from_iter(params.iter()))
            .map_err(|e| format!("Execute error: {}", e))?;

        let tag = match cmd {
            "INSERT" => format!("INSERT 0 {}", affected),
            "UPDATE" => format!("UPDATE {}", affected),
            "DELETE" => format!("DELETE {}", affected),
            _ => format!("{} {}", cmd, affected),
        };

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: affected as u64,
            command_tag: tag,
        })
    }

    fn execute_query(conn: &Connection, sql: &str) -> Result<QueryResult, String> {
        let mut stmt = conn.prepare(sql)
            .map_err(|e| format!("Prepare error: {}", e))?;

        let column_count = stmt.column_count();
        // Infer column types from declared types
        let columns: Vec<ColumnInfo> = (0..column_count)
            .map(|i| {
                let name = stmt.column_name(i).unwrap_or("?").to_string();
                let type_oid = stmt.columns()[i].decl_type()
                    .map(decltype_to_oid)
                    .unwrap_or(OID_TEXT);
                ColumnInfo { name, type_oid }
            })
            .collect();

        let rows: Vec<Vec<Option<String>>> = stmt.query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..column_count {
                let val: Option<String> = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => None,
                    Ok(rusqlite::types::ValueRef::Integer(n)) => Some(n.to_string()),
                    Ok(rusqlite::types::ValueRef::Real(f)) => Some(f.to_string()),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        Some(String::from_utf8_lossy(s).to_string())
                    }
                    Ok(rusqlite::types::ValueRef::Blob(b)) => {
                        Some(format!("\\x{}", hex::encode(b)))
                    }
                    Err(_) => None,
                };
                vals.push(val);
            }
            Ok(vals)
        })
        .map_err(|e| format!("Query error: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

        let row_count = rows.len();
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: row_count as u64,
            command_tag: format!("SELECT {}", row_count),
        })
    }

    fn has_returning_clause(upper_sql: &str) -> bool {
        // Check for RETURNING keyword (not inside a string)
        upper_sql.contains(" RETURNING ") || upper_sql.ends_with(" RETURNING *")
            || upper_sql.ends_with(" RETURNING")
    }

    /// Execute a DML statement with RETURNING clause (treated as a query)
    fn execute_returning(conn: &Connection, sql: &str, cmd: &str) -> Result<QueryResult, String> {
        let mut stmt = conn.prepare(sql)
            .map_err(|e| format!("Prepare error: {}", e))?;
        let column_count = stmt.column_count();
        let columns: Vec<ColumnInfo> = (0..column_count)
            .map(|i| {
                let name = stmt.column_name(i).unwrap_or("?").to_string();
                let type_oid = stmt.columns()[i].decl_type()
                    .map(decltype_to_oid)
                    .unwrap_or(OID_TEXT);
                ColumnInfo { name, type_oid }
            })
            .collect();

        let rows: Vec<Vec<Option<String>>> = stmt.query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..column_count {
                let val: Option<String> = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => None,
                    Ok(rusqlite::types::ValueRef::Integer(n)) => Some(n.to_string()),
                    Ok(rusqlite::types::ValueRef::Real(f)) => Some(f.to_string()),
                    Ok(rusqlite::types::ValueRef::Text(s)) => Some(String::from_utf8_lossy(s).to_string()),
                    Ok(rusqlite::types::ValueRef::Blob(b)) => Some(format!("\\x{}", hex::encode(b))),
                    Err(_) => None,
                };
                vals.push(val);
            }
            Ok(vals)
        })
        .map_err(|e| format!("Query error: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

        let row_count = rows.len();
        let tag = match cmd {
            "INSERT" => format!("INSERT 0 {}", row_count),
            "UPDATE" => format!("UPDATE {}", row_count),
            "DELETE" => format!("DELETE {}", row_count),
            _ => format!("{} {}", cmd, row_count),
        };
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: row_count as u64,
            command_tag: tag,
        })
    }

    fn execute_returning_with_params(
        conn: &Connection,
        sql: &str,
        cmd: &str,
        params: &[&dyn rusqlite::types::ToSql],
    ) -> Result<QueryResult, String> {
        let mut stmt = conn.prepare(sql)
            .map_err(|e| format!("Prepare error: {}", e))?;
        let column_count = stmt.column_count();
        let columns: Vec<ColumnInfo> = (0..column_count)
            .map(|i| {
                let name = stmt.column_name(i).unwrap_or("?").to_string();
                let type_oid = stmt.columns()[i].decl_type()
                    .map(decltype_to_oid)
                    .unwrap_or(OID_TEXT);
                ColumnInfo { name, type_oid }
            })
            .collect();

        let rows: Vec<Vec<Option<String>>> = stmt.query_map(
            rusqlite::params_from_iter(params.iter()),
            |row| {
                let mut vals = Vec::new();
                for i in 0..column_count {
                    let val: Option<String> = match row.get_ref(i) {
                        Ok(rusqlite::types::ValueRef::Null) => None,
                        Ok(rusqlite::types::ValueRef::Integer(n)) => Some(n.to_string()),
                        Ok(rusqlite::types::ValueRef::Real(f)) => Some(f.to_string()),
                        Ok(rusqlite::types::ValueRef::Text(s)) => Some(String::from_utf8_lossy(s).to_string()),
                        Ok(rusqlite::types::ValueRef::Blob(b)) => Some(format!("\\x{}", hex::encode(b))),
                        Err(_) => None,
                    };
                    vals.push(val);
                }
                Ok(vals)
            },
        )
        .map_err(|e| format!("Query error: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

        let row_count = rows.len();
        let tag = match cmd {
            "INSERT" => format!("INSERT 0 {}", row_count),
            "UPDATE" => format!("UPDATE {}", row_count),
            "DELETE" => format!("DELETE {}", row_count),
            _ => format!("{} {}", cmd, row_count),
        };
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: row_count as u64,
            command_tag: tag,
        })
    }

    fn execute_modify(conn: &Connection, sql: &str, cmd: &str) -> Result<QueryResult, String> {
        let affected = conn.execute(sql, [])
            .map_err(|e| format!("Execute error: {}", e))?;

        let tag = match cmd {
            "INSERT" => format!("INSERT 0 {}", affected),
            "UPDATE" => format!("UPDATE {}", affected),
            "DELETE" => format!("DELETE {}", affected),
            _ => format!("{} {}", cmd, affected),
        };

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: affected as u64,
            command_tag: tag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_backend() -> SqliteBackend {
        SqliteBackend::new(":memory:").unwrap()
    }

    // ========================================================================
    // BASIC CONNECTIVITY
    // ========================================================================

    #[test]
    fn test_backend_creates_in_memory() {
        let backend = new_backend();
        let result = backend.execute("SELECT 1").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("1".to_string()));
    }

    #[test]
    fn test_backend_creates_successfully() {
        // WAL mode is set on creation; in-memory DBs use "memory" mode
        // Just verify the backend works after initialization
        let backend = new_backend();
        let result = backend.execute("SELECT 1 + 1").unwrap();
        assert_eq!(result.rows[0][0], Some("2".to_string()));
    }

    // ========================================================================
    // DDL WITH PG TYPES
    // ========================================================================

    #[test]
    fn test_create_table_with_pg_types() {
        let backend = new_backend();
        let result = backend.execute(
            "CREATE TABLE users (
                id UUID PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email TEXT NOT NULL,
                active BOOLEAN DEFAULT 'true',
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT (datetime('now'))
            )"
        );
        assert!(result.is_ok(), "CREATE TABLE with PG types should work: {:?}", result.err());
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        let result = backend.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER)");
        assert!(result.is_ok());
    }

    // ========================================================================
    // CRUD OPERATIONS
    // ========================================================================

    #[test]
    fn test_insert_and_select() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        let insert = backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        assert_eq!(insert.rows_affected, 1);
        assert!(insert.command_tag.contains("INSERT"));

        let select = backend.execute("SELECT id, name FROM t").unwrap();
        assert_eq!(select.rows.len(), 1);
        assert_eq!(select.rows[0][0], Some("1".to_string()));
        assert_eq!(select.rows[0][1], Some("alice".to_string()));
        assert_eq!(select.columns[0].name, "id");
        assert_eq!(select.columns[1].name, "name");
    }

    #[test]
    fn test_update() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        let update = backend.execute("UPDATE t SET name = 'bob' WHERE id = 1").unwrap();
        assert_eq!(update.rows_affected, 1);
        assert!(update.command_tag.contains("UPDATE"));

        let select = backend.execute("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(select.rows[0][0], Some("bob".to_string()));
    }

    #[test]
    fn test_delete() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        let delete = backend.execute("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(delete.rows_affected, 1);
        assert!(delete.command_tag.contains("DELETE"));
    }

    #[test]
    fn test_null_values() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        let select = backend.execute("SELECT val FROM t").unwrap();
        assert_eq!(select.rows[0][0], None);
    }

    // ========================================================================
    // FUNCTION EMULATION
    // ========================================================================

    #[test]
    fn test_uuid_generation() {
        let backend = new_backend();
        let result = backend.execute("SELECT pgsqlite_uuid_v4()").unwrap();
        let uuid = result.rows[0][0].as_ref().unwrap();
        // UUID format: 8-4-4-4-12 hex chars
        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn test_now_function_translated() {
        let backend = new_backend();
        let result = backend.execute("SELECT now()").unwrap();
        let ts = result.rows[0][0].as_ref().unwrap();
        // Should be a datetime string
        assert!(ts.contains("-"), "now() should return a date, got: {}", ts);
    }

    // ========================================================================
    // PG SQL COMPATIBILITY
    // ========================================================================

    #[test]
    fn test_pg_style_cast() {
        let backend = new_backend();
        let result = backend.execute("SELECT '42'::INTEGER").unwrap();
        assert_eq!(result.rows[0][0], Some("42".to_string()));
    }

    #[test]
    fn test_command_tags() {
        let backend = new_backend();
        let create = backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        assert_eq!(create.command_tag, "CREATE TABLE");

        let insert = backend.execute("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(insert.command_tag, "INSERT 0 1");

        let select = backend.execute("SELECT * FROM t").unwrap();
        assert_eq!(select.command_tag, "SELECT 1");
    }

    #[test]
    fn test_set_command_ignored() {
        let backend = new_backend();
        let result = backend.execute("SET client_encoding TO 'UTF8'");
        assert!(result.is_ok(), "SET commands should be silently accepted");
    }

    #[test]
    fn test_multiple_rows() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        backend.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        backend.execute("INSERT INTO t VALUES (3, 'charlie')").unwrap();

        let select = backend.execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(select.rows.len(), 3);
        assert_eq!(select.command_tag, "SELECT 3");
    }

    #[test]
    fn test_sql_error_returns_err() {
        let backend = new_backend();
        let result = backend.execute("SELECT * FROM nonexistent_table");
        assert!(result.is_err());
    }

    // ========================================================================
    // PARAMETERIZED QUERIES
    // ========================================================================

    #[test]
    fn test_parameterized_select() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        backend.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let result = backend.execute_with_params(
            "SELECT name FROM t WHERE id = $1",
            &[Some(b"1".to_vec())],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("alice".to_string()));
    }

    #[test]
    fn test_parameterized_insert() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let result = backend.execute_with_params(
            "INSERT INTO t (id, name) VALUES ($1, $2)",
            &[Some(b"42".to_vec()), Some(b"charlie".to_vec())],
        ).unwrap();
        assert_eq!(result.rows_affected, 1);

        let select = backend.execute("SELECT * FROM t WHERE id = 42").unwrap();
        assert_eq!(select.rows[0][1], Some("charlie".to_string()));
    }

    #[test]
    fn test_parameterized_update() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let result = backend.execute_with_params(
            "UPDATE t SET name = $1 WHERE id = $2",
            &[Some(b"ALICE".to_vec()), Some(b"1".to_vec())],
        ).unwrap();
        assert_eq!(result.rows_affected, 1);

        let select = backend.execute("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(select.rows[0][0], Some("ALICE".to_string()));
    }

    #[test]
    fn test_parameterized_delete() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let result = backend.execute_with_params(
            "DELETE FROM t WHERE id = $1",
            &[Some(b"1".to_vec())],
        ).unwrap();
        assert_eq!(result.rows_affected, 1);
    }

    #[test]
    fn test_parameterized_null() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();

        backend.execute_with_params(
            "INSERT INTO t (id, val) VALUES ($1, $2)",
            &[Some(b"1".to_vec()), None],
        ).unwrap();

        let select = backend.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(select.rows[0][0], None);
    }

    #[test]
    fn test_parameterized_multiple_params() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (a TEXT, b TEXT, c TEXT)").unwrap();

        backend.execute_with_params(
            "INSERT INTO t VALUES ($1, $2, $3)",
            &[Some(b"x".to_vec()), Some(b"y".to_vec()), Some(b"z".to_vec())],
        ).unwrap();

        let select = backend.execute("SELECT * FROM t").unwrap();
        assert_eq!(select.rows[0], vec![
            Some("x".to_string()),
            Some("y".to_string()),
            Some("z".to_string()),
        ]);
    }

    // ========================================================================
    // TYPE OID INFERENCE
    // ========================================================================

    #[test]
    fn test_integer_column_returns_int4_oid() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        backend.execute("INSERT INTO t VALUES (1)").unwrap();
        let result = backend.execute("SELECT id FROM t").unwrap();
        assert_eq!(result.columns[0].type_oid, OID_INT4);
    }

    #[test]
    fn test_bigint_column_returns_int8_oid() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id BIGINT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1)").unwrap();
        let result = backend.execute("SELECT id FROM t").unwrap();
        assert_eq!(result.columns[0].type_oid, OID_INT8);
    }

    #[test]
    fn test_real_column_returns_float4_oid() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (val REAL)").unwrap();
        backend.execute("INSERT INTO t VALUES (1.5)").unwrap();
        let result = backend.execute("SELECT val FROM t").unwrap();
        assert_eq!(result.columns[0].type_oid, OID_FLOAT4);
    }

    #[test]
    fn test_text_column_returns_text_oid() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES ('hello')").unwrap();
        let result = backend.execute("SELECT name FROM t").unwrap();
        assert_eq!(result.columns[0].type_oid, OID_TEXT);
    }

    #[test]
    fn test_blob_column_returns_bytea_oid() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (data BLOB)").unwrap();
        let result = backend.execute("SELECT data FROM t").unwrap();
        assert_eq!(result.columns[0].type_oid, OID_BYTEA);
    }

    #[test]
    fn test_boolean_column_type_from_pg() {
        let backend = new_backend();
        // PG BOOLEAN gets translated to TEXT by our translator
        backend.execute("CREATE TABLE t (active BOOLEAN)").unwrap();
        let result = backend.execute("SELECT active FROM t").unwrap();
        // After translation, BOOLEAN becomes TEXT
        assert_eq!(result.columns[0].type_oid, OID_TEXT);
    }

    #[test]
    fn test_multiple_column_types() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT, val REAL, data BLOB)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'a', 1.5, X'FF')").unwrap();
        let result = backend.execute("SELECT id, name, val, data FROM t").unwrap();
        assert_eq!(result.columns[0].type_oid, OID_INT4);
        assert_eq!(result.columns[1].type_oid, OID_TEXT);
        assert_eq!(result.columns[2].type_oid, OID_FLOAT4);
        assert_eq!(result.columns[3].type_oid, OID_BYTEA);
    }

    #[test]
    fn test_describe_query_column_types() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        let columns = backend.describe_query("SELECT id, name FROM t").unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].type_oid, OID_INT4);
        assert_eq!(columns[1].type_oid, OID_TEXT);
    }

    // ========================================================================
    // TRANSACTION CONTROL
    // ========================================================================

    #[test]
    fn test_begin_commit_transaction() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        backend.execute("BEGIN").unwrap();
        backend.execute("INSERT INTO t VALUES (1)").unwrap();
        backend.execute("COMMIT").unwrap();
        let result = backend.execute("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_begin_rollback_transaction() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        backend.execute("INSERT INTO t VALUES (1)").unwrap();
        backend.execute("BEGIN").unwrap();
        backend.execute("INSERT INTO t VALUES (2)").unwrap();
        backend.execute("ROLLBACK").unwrap();
        let result = backend.execute("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_start_transaction_syntax() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        let result = backend.execute("START TRANSACTION");
        assert!(result.is_ok(), "START TRANSACTION should work: {:?}", result.err());
        backend.execute("ROLLBACK").unwrap();
    }

    // ========================================================================
    // FUNCTION EMULATION END-TO-END
    // ========================================================================

    #[test]
    fn test_coalesce_e2e() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (val TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (NULL)").unwrap();
        let result = backend.execute("SELECT COALESCE(val, 'default') FROM t").unwrap();
        assert_eq!(result.rows[0][0], Some("default".to_string()));
    }

    #[test]
    fn test_lower_upper_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT lower('HELLO'), upper('hello')").unwrap();
        assert_eq!(result.rows[0][0], Some("hello".to_string()));
        assert_eq!(result.rows[0][1], Some("HELLO".to_string()));
    }

    #[test]
    fn test_length_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT length('hello')").unwrap();
        assert_eq!(result.rows[0][0], Some("5".to_string()));
    }

    #[test]
    fn test_replace_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT replace('hello world', 'world', 'rust')").unwrap();
        assert_eq!(result.rows[0][0], Some("hello rust".to_string()));
    }

    #[test]
    fn test_abs_round_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT abs(-42), round(3.7)").unwrap();
        assert_eq!(result.rows[0][0], Some("42".to_string()));
        assert_eq!(result.rows[0][1], Some("4".to_string()));
    }

    #[test]
    fn test_substr_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT substr('hello', 1, 3)").unwrap();
        assert_eq!(result.rows[0][0], Some("hel".to_string()));
    }

    #[test]
    fn test_trim_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT trim('  hello  ')").unwrap();
        assert_eq!(result.rows[0][0], Some("hello".to_string()));
    }

    #[test]
    fn test_date_trunc_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT date_trunc('month', '2024-03-15 14:30:00')").unwrap();
        assert_eq!(result.rows[0][0], Some("2024-03-01 00:00:00".to_string()));
    }

    #[test]
    fn test_to_char_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT to_char('2024-03-15 14:30:00', 'YYYY-MM-DD')").unwrap();
        assert_eq!(result.rows[0][0], Some("2024-03-15".to_string()));
    }

    #[test]
    fn test_extract_epoch_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT EXTRACT(EPOCH FROM '2024-01-01 00:00:00')").unwrap();
        let epoch: i64 = result.rows[0][0].as_ref().unwrap().parse().unwrap();
        assert!(epoch > 1700000000, "Epoch should be after 2023, got: {}", epoch);
    }

    #[test]
    fn test_extract_year_e2e() {
        let backend = new_backend();
        let result = backend.execute("SELECT EXTRACT(YEAR FROM '2024-03-15')").unwrap();
        assert_eq!(result.rows[0][0], Some("2024".to_string()));
    }

    // ========================================================================
    // RETURNING CLAUSE
    // ========================================================================

    #[test]
    fn test_insert_returning() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let result = backend.execute("INSERT INTO t (id, name) VALUES (1, 'alice') RETURNING id, name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("1".to_string()));
        assert_eq!(result.rows[0][1], Some("alice".to_string()));
        assert!(!result.columns.is_empty());
        assert_eq!(result.columns[0].name, "id");
    }

    #[test]
    fn test_insert_returning_star() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let result = backend.execute("INSERT INTO t VALUES (1, 'alice') RETURNING *").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn test_update_returning() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        let result = backend.execute("UPDATE t SET name = 'bob' WHERE id = 1 RETURNING id, name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("bob".to_string()));
    }

    #[test]
    fn test_delete_returning() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        let result = backend.execute("DELETE FROM t WHERE id = 1 RETURNING *").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("alice".to_string()));
    }

    #[test]
    fn test_insert_returning_with_params() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let result = backend.execute_with_params(
            "INSERT INTO t (id, name) VALUES ($1, $2) RETURNING id, name",
            &[Some(b"42".to_vec()), Some(b"charlie".to_vec())],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("42".to_string()));
        assert_eq!(result.rows[0][1], Some("charlie".to_string()));
    }

    // ========================================================================
    // UPSERT (ON CONFLICT)
    // ========================================================================

    #[test]
    fn test_upsert_on_conflict_do_nothing() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let result = backend.execute(
            "INSERT INTO t (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING"
        );
        assert!(result.is_ok(), "ON CONFLICT DO NOTHING should work: {:?}", result.err());

        // Original value should remain
        let select = backend.execute("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(select.rows[0][0], Some("alice".to_string()));
    }

    #[test]
    fn test_upsert_on_conflict_do_update() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let result = backend.execute(
            "INSERT INTO t (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = excluded.name"
        );
        assert!(result.is_ok(), "ON CONFLICT DO UPDATE should work: {:?}", result.err());

        let select = backend.execute("SELECT name FROM t WHERE id = 1").unwrap();
        assert_eq!(select.rows[0][0], Some("bob".to_string()));
    }

    #[test]
    fn test_upsert_with_returning() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let result = backend.execute(
            "INSERT INTO t (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = excluded.name RETURNING id, name"
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("bob".to_string()));
    }

    // ========================================================================
    // EDGE CASES
    // ========================================================================

    #[test]
    fn test_empty_table_select() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        let result = backend.execute("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 0);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.command_tag, "SELECT 0");
    }

    #[test]
    fn test_select_literal_expressions() {
        let backend = new_backend();
        let result = backend.execute("SELECT 1, 'hello', 3.14").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("1".to_string()));
        assert_eq!(result.rows[0][1], Some("hello".to_string()));
        assert_eq!(result.rows[0][2], Some("3.14".to_string()));
    }

    #[test]
    fn test_unicode_data() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES ('Hello 🌍')").unwrap();
        backend.execute("INSERT INTO t VALUES ('日本語')").unwrap();
        let result = backend.execute("SELECT name FROM t ORDER BY rowid").unwrap();
        assert_eq!(result.rows[0][0], Some("Hello 🌍".to_string()));
        assert_eq!(result.rows[1][0], Some("日本語".to_string()));
    }

    #[test]
    fn test_very_long_string() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (data TEXT)").unwrap();
        let long_string = "x".repeat(10000);
        backend.execute(&format!("INSERT INTO t VALUES ('{}')", long_string)).unwrap();
        let result = backend.execute("SELECT data FROM t").unwrap();
        assert_eq!(result.rows[0][0].as_ref().unwrap().len(), 10000);
    }

    #[test]
    fn test_drop_table() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        backend.execute("INSERT INTO t VALUES (1)").unwrap();
        let result = backend.execute("DROP TABLE t");
        assert!(result.is_ok());
        // Table should be gone
        let err = backend.execute("SELECT * FROM t");
        assert!(err.is_err());
    }

    #[test]
    fn test_alter_table_add_column() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER)").unwrap();
        backend.execute("INSERT INTO t VALUES (1)").unwrap();
        // ALTER TABLE - passes through to SQLite
        backend.execute("ALTER TABLE t ADD COLUMN name TEXT").unwrap();
        let result = backend.execute("SELECT id, name FROM t").unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows[0][1], None); // new column has NULL
    }

    #[test]
    fn test_case_expression() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, status TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'active')").unwrap();
        backend.execute("INSERT INTO t VALUES (2, 'inactive')").unwrap();

        let result = backend.execute(
            "SELECT id, CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END AS is_active FROM t ORDER BY id"
        ).unwrap();
        assert_eq!(result.rows[0][1], Some("yes".to_string()));
        assert_eq!(result.rows[1][1], Some("no".to_string()));
    }

    #[test]
    fn test_group_by_aggregate() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (category TEXT, amount INTEGER)").unwrap();
        backend.execute("INSERT INTO t VALUES ('a', 10)").unwrap();
        backend.execute("INSERT INTO t VALUES ('a', 20)").unwrap();
        backend.execute("INSERT INTO t VALUES ('b', 30)").unwrap();

        let result = backend.execute(
            "SELECT category, sum(amount) as total FROM t GROUP BY category ORDER BY category"
        ).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Some("30".to_string()));
        assert_eq!(result.rows[1][1], Some("30".to_string()));
    }

    #[test]
    fn test_subquery() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        backend.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let result = backend.execute(
            "SELECT name FROM t WHERE id IN (SELECT id FROM t WHERE name = 'alice')"
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("alice".to_string()));
    }

    #[test]
    fn test_join_query() {
        let backend = new_backend();
        backend.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        backend.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)").unwrap();
        backend.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        backend.execute("INSERT INTO orders VALUES (1, 1, 'widget')").unwrap();

        let result = backend.execute(
            "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id"
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("alice".to_string()));
        assert_eq!(result.rows[0][1], Some("widget".to_string()));
    }

    #[test]
    fn test_query_sqlite_master_tables() {
        let backend = new_backend();
        backend.execute("CREATE TABLE alpha (id INTEGER)").unwrap();
        backend.execute("CREATE TABLE beta (id INTEGER)").unwrap();

        let tables = backend.query_sqlite_master_tables();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"alpha".to_string()));
        assert!(tables.contains(&"beta".to_string()));
    }

    #[test]
    fn test_query_sqlite_master_columns() {
        let backend = new_backend();
        backend.execute("CREATE TABLE t (id INTEGER NOT NULL, name TEXT, val REAL)").unwrap();

        let columns = backend.query_sqlite_master_columns();
        assert_eq!(columns.len(), 3);

        // Find the id column
        let id_col = columns.iter().find(|c| c.1 == "id").unwrap();
        assert_eq!(id_col.0, "t");      // table name
        assert_eq!(id_col.2, "INTEGER"); // type
        assert_eq!(id_col.3, false);     // NOT nullable
        assert_eq!(id_col.4, 1);        // ordinal_position (1-based)
    }

    #[test]
    fn test_copy_from_stdin_basic() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE items (id INTEGER, name TEXT, price REAL)").unwrap();

        let columns = vec!["id".to_string(), "name".to_string(), "price".to_string()];
        let rows = vec![
            vec!["1".to_string(), "apple".to_string(), "1.50".to_string()],
            vec!["2".to_string(), "banana".to_string(), "0.75".to_string()],
            vec!["3".to_string(), "cherry".to_string(), "3.00".to_string()],
        ];

        let count = backend.copy_from_stdin("items", &columns, &rows, '\t').unwrap();
        assert_eq!(count, 3);

        let result = backend.execute("SELECT * FROM items ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], Some("apple".to_string()));
        assert_eq!(result.rows[2][1], Some("cherry".to_string()));
    }

    #[test]
    fn test_copy_from_stdin_no_columns() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE simple (a TEXT, b TEXT)").unwrap();

        let columns: Vec<String> = vec![];
        let rows = vec![
            vec!["hello".to_string(), "world".to_string()],
        ];

        let count = backend.copy_from_stdin("simple", &columns, &rows, '\t').unwrap();
        assert_eq!(count, 1);

        let result = backend.execute("SELECT * FROM simple").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("hello".to_string()));
    }

    #[test]
    fn test_copy_from_stdin_empty_data() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE empty_copy (id INTEGER)").unwrap();

        let columns = vec!["id".to_string()];
        let rows: Vec<Vec<String>> = vec![];

        let count = backend.copy_from_stdin("empty_copy", &columns, &rows, '\t').unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_truncate_table() {
        let backend = new_backend();
        backend.execute("CREATE TABLE trunc (id INTEGER, name TEXT)").unwrap();
        backend.execute("INSERT INTO trunc VALUES (1, 'a')").unwrap();
        backend.execute("INSERT INTO trunc VALUES (2, 'b')").unwrap();

        let result = backend.execute("TRUNCATE TABLE trunc").unwrap();
        assert_eq!(result.command_tag, "TRUNCATE TABLE");

        let select = backend.execute("SELECT * FROM trunc").unwrap();
        assert_eq!(select.rows.len(), 0);
    }

    #[test]
    fn test_savepoint_release() {
        let backend = new_backend();
        backend.execute("CREATE TABLE sp_test (id INTEGER, val TEXT)").unwrap();
        backend.execute("BEGIN").unwrap();
        backend.execute("INSERT INTO sp_test VALUES (1, 'a')").unwrap();
        backend.execute("SAVEPOINT sp1").unwrap();
        backend.execute("INSERT INTO sp_test VALUES (2, 'b')").unwrap();
        backend.execute("RELEASE SAVEPOINT sp1").unwrap();
        backend.execute("COMMIT").unwrap();

        let result = backend.execute("SELECT * FROM sp_test ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_savepoint_rollback() {
        let backend = new_backend();
        backend.execute("CREATE TABLE sp_rb (id INTEGER, val TEXT)").unwrap();
        backend.execute("BEGIN").unwrap();
        backend.execute("INSERT INTO sp_rb VALUES (1, 'a')").unwrap();
        backend.execute("SAVEPOINT sp1").unwrap();
        backend.execute("INSERT INTO sp_rb VALUES (2, 'b')").unwrap();
        backend.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
        backend.execute("COMMIT").unwrap();

        let result = backend.execute("SELECT * FROM sp_rb ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 1, "Should only have 1 row after rollback to savepoint");
        assert_eq!(result.rows[0][1], Some("a".to_string()));
    }

    #[test]
    fn test_nested_savepoints() {
        let backend = new_backend();
        backend.execute("CREATE TABLE sp_nest (id INTEGER)").unwrap();
        backend.execute("BEGIN").unwrap();
        backend.execute("INSERT INTO sp_nest VALUES (1)").unwrap();
        backend.execute("SAVEPOINT sp1").unwrap();
        backend.execute("INSERT INTO sp_nest VALUES (2)").unwrap();
        backend.execute("SAVEPOINT sp2").unwrap();
        backend.execute("INSERT INTO sp_nest VALUES (3)").unwrap();
        backend.execute("ROLLBACK TO SAVEPOINT sp2").unwrap();
        // Row 3 should be gone, but 1 and 2 remain
        backend.execute("RELEASE SAVEPOINT sp1").unwrap();
        backend.execute("COMMIT").unwrap();

        let result = backend.execute("SELECT * FROM sp_nest ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_query_sqlite_master_indexes() {
        let backend = new_backend();
        backend.execute("CREATE TABLE idx_test (id INTEGER, name TEXT, email TEXT)").unwrap();
        backend.execute("CREATE INDEX idx_name ON idx_test (name)").unwrap();
        backend.execute("CREATE UNIQUE INDEX idx_email ON idx_test (email)").unwrap();

        let indexes = backend.query_sqlite_master_indexes();
        assert_eq!(indexes.len(), 2);
        let names: Vec<&str> = indexes.iter().map(|i| i.0.as_str()).collect();
        assert!(names.contains(&"idx_name"));
        assert!(names.contains(&"idx_email"));
        // All indexes should reference idx_test
        for (_, tbl, _) in &indexes {
            assert_eq!(tbl, "idx_test");
        }
    }

    #[test]
    fn test_query_constraints() {
        let backend = new_backend();
        backend.execute("CREATE TABLE con_test (id INTEGER PRIMARY KEY NOT NULL, name TEXT, email TEXT NOT NULL)").unwrap();

        let constraints = backend.query_constraints();
        // Should have: id PRIMARY KEY, id NOT NULL, email NOT NULL
        let pk_count = constraints.iter().filter(|c| c.2 == "PRIMARY KEY").count();
        let nn_count = constraints.iter().filter(|c| c.2 == "NOT NULL").count();
        assert!(pk_count >= 1, "Should have at least 1 PRIMARY KEY constraint");
        assert!(nn_count >= 2, "Should have at least 2 NOT NULL constraints");
    }

    #[test]
    fn test_serial_autoincrement() {
        let backend = new_backend();
        backend.execute("CREATE TABLE serial_test (id SERIAL, name TEXT)").unwrap();
        backend.execute("INSERT INTO serial_test (name) VALUES ('alice')").unwrap();
        backend.execute("INSERT INTO serial_test (name) VALUES ('bob')").unwrap();

        let result = backend.execute("SELECT * FROM serial_test ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], Some("alice".to_string()));
        assert_eq!(result.rows[1][1], Some("bob".to_string()));
        // IDs should be auto-assigned
        let id1 = result.rows[0][0].as_ref().unwrap();
        let id2 = result.rows[1][0].as_ref().unwrap();
        assert_ne!(id1, id2, "Auto-generated IDs should be different");
    }
}
