use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use bytes::Bytes;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::backend::sqlite::SqliteBackend;
use crate::catalog;
use crate::protocol::messages::*;

pub struct PgServer {
    backend: Arc<SqliteBackend>,
    addr: String,
}

impl PgServer {
    pub fn new(addr: &str, db_path: &str) -> Result<Self, String> {
        let backend = SqliteBackend::new(db_path)?;
        Ok(Self {
            backend: Arc::new(backend),
            addr: addr.to_string(),
        })
    }

    pub async fn run(&self) -> Result<(), String> {
        let listener = TcpListener::bind(&self.addr).await
            .map_err(|e| format!("Failed to bind: {}", e))?;
        tracing::info!("pgsqlite listening on {}", self.addr);

        loop {
            let (stream, addr) = listener.accept().await
                .map_err(|e| format!("Accept error: {}", e))?;
            tracing::info!("New connection from {}", addr);

            let backend = self.backend.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, backend).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}

/// Public wrapper for integration tests
pub async fn handle_connection_public(stream: TcpStream, backend: Arc<SqliteBackend>) -> Result<(), String> {
    handle_connection(stream, backend).await
}

struct PreparedStatement {
    query: String,
    param_types: Vec<i32>,
}

struct Portal {
    query: String,
    params: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TransactionState {
    Idle,         // 'I' - not in a transaction
    InTransaction, // 'T' - in a transaction block
    Failed,       // 'E' - in a failed transaction block
}

impl TransactionState {
    fn status_byte(&self) -> u8 {
        match self {
            TransactionState::Idle => b'I',
            TransactionState::InTransaction => b'T',
            TransactionState::Failed => b'E',
        }
    }
}

/// Maximum allowed message size (100MB). Prevents OOM from malicious/corrupted messages.
const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

struct ConnectionState {
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    transaction_state: TransactionState,
    /// SQL-level PREPARE'd statements (PREPARE name AS query)
    sql_prepared: HashMap<String, String>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            transaction_state: TransactionState::Idle,
            sql_prepared: HashMap::new(),
        }
    }
}

async fn handle_connection(mut stream: TcpStream, backend: Arc<SqliteBackend>) -> Result<(), String> {
    handle_startup(&mut stream).await?;
    send_startup_response(&mut stream).await?;

    let mut state = ConnectionState::new();

    loop {
        let mut header = [0u8; 5];
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::info!("Client disconnected");
                return Ok(());
            }
            Err(e) => return Err(format!("Read error: {}", e)),
        }

        let msg_type = header[0];
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

        if length > MAX_MESSAGE_SIZE {
            let error = BackendMessage::ErrorResponse {
                severity: "FATAL".to_string(),
                code: "08P01".to_string(),
                message: format!("Message too large: {} bytes (max {})", length, MAX_MESSAGE_SIZE),
            };
            let _ = stream.write_all(&error.serialize()).await;
            return Err(format!("Message too large: {} bytes", length));
        }

        let body_len = length.saturating_sub(4);
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            stream.read_exact(&mut body).await
                .map_err(|e| format!("Read body error: {}", e))?;
        }

        let mut full_msg = Vec::with_capacity(5 + body_len);
        full_msg.push(msg_type);
        full_msg.extend_from_slice(&(length as i32).to_be_bytes());
        full_msg.extend_from_slice(&body);

        let msg = FrontendMessage::parse(&Bytes::from(full_msg))
            .map_err(|e| format!("Parse error: {}", e))?;

        match msg {
            FrontendMessage::Query(sql) => {
                handle_simple_query(&mut stream, &backend, &sql, &mut state).await?;
            }
            FrontendMessage::Parse { name, query, param_types } => {
                handle_parse(&mut stream, &mut state, name, query, param_types).await?;
            }
            FrontendMessage::Bind { portal, statement, params, .. } => {
                handle_bind(&mut stream, &mut state, portal, statement, params).await?;
            }
            FrontendMessage::Describe { kind, name } => {
                handle_describe(&mut stream, &backend, &state, kind, &name).await?;
            }
            FrontendMessage::Execute { portal, max_rows } => {
                handle_execute(&mut stream, &backend, &mut state, &portal, max_rows).await?;
            }
            FrontendMessage::Sync => {
                // If in Failed state, auto-rollback before sending ReadyForQuery
                if state.transaction_state == TransactionState::Failed {
                    let _ = backend.execute("ROLLBACK");
                    state.transaction_state = TransactionState::Idle;
                }
                let resp = BackendMessage::ReadyForQuery {
                    status: state.transaction_state.status_byte(),
                };
                stream.write_all(&resp.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
            }
            FrontendMessage::Terminate => {
                tracing::info!("Client terminated");
                return Ok(());
            }
            FrontendMessage::Close { kind, name } => {
                match kind {
                    DescribeKind::Statement => { state.prepared_statements.remove(&name); }
                    DescribeKind::Portal => { state.portals.remove(&name); }
                }
                // CloseComplete = '3', length 4
                let mut buf = vec![b'3'];
                buf.extend_from_slice(&4_i32.to_be_bytes());
                stream.write_all(&buf).await
                    .map_err(|e| format!("Write error: {}", e))?;
            }
            FrontendMessage::Flush => {
                stream.flush().await
                    .map_err(|e| format!("Flush error: {}", e))?;
            }
            _ => {
                tracing::warn!("Unhandled message type");
            }
        }
    }
}

/// Count the number of $N parameter placeholders in a query
fn count_params(query: &str) -> usize {
    let mut max_param = 0usize;
    let mut chars = query.chars().peekable();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '$' if !in_single_quote && !in_double_quote => {
                let mut digits = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() {
                        digits.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if let Ok(n) = digits.parse::<usize>() {
                    if n > max_param {
                        max_param = n;
                    }
                }
            }
            _ => {}
        }
    }
    max_param
}

async fn handle_parse(
    stream: &mut TcpStream,
    state: &mut ConnectionState,
    name: String,
    query: String,
    param_types: Vec<i32>,
) -> Result<(), String> {
    // If client didn't specify param types, infer count from $N placeholders
    // Use OID 25 (TEXT) as default - SQLite treats everything as text anyway
    let final_param_types = if param_types.is_empty() {
        let count = count_params(&query);
        vec![OID_TEXT; count]
    } else {
        param_types
    };

    state.prepared_statements.insert(name, PreparedStatement {
        query,
        param_types: final_param_types,
    });
    let resp = BackendMessage::ParseComplete;
    stream.write_all(&resp.serialize()).await
        .map_err(|e| format!("Write error: {}", e))
}

async fn handle_bind(
    stream: &mut TcpStream,
    state: &mut ConnectionState,
    portal_name: String,
    statement_name: String,
    params: Vec<Option<Vec<u8>>>,
) -> Result<(), String> {
    let query = state.prepared_statements
        .get(&statement_name)
        .map(|s| s.query.clone())
        .unwrap_or_default();

    state.portals.insert(portal_name, Portal { query, params });

    let resp = BackendMessage::BindComplete;
    stream.write_all(&resp.serialize()).await
        .map_err(|e| format!("Write error: {}", e))
}

async fn handle_describe(
    stream: &mut TcpStream,
    backend: &SqliteBackend,
    state: &ConnectionState,
    kind: DescribeKind,
    name: &str,
) -> Result<(), String> {
    let query = match kind {
        DescribeKind::Statement => {
            if let Some(stmt) = state.prepared_statements.get(name) {
                // Send ParameterDescription first
                let param_desc = BackendMessage::ParameterDescription {
                    param_types: stmt.param_types.clone(),
                };
                stream.write_all(&param_desc.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                Some(stmt.query.clone())
            } else {
                // Unknown statement - send empty param desc
                let param_desc = BackendMessage::ParameterDescription {
                    param_types: vec![],
                };
                stream.write_all(&param_desc.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                None
            }
        }
        DescribeKind::Portal => {
            state.portals.get(name).map(|p| p.query.clone())
        }
    };

    if let Some(ref sql) = query {
        let upper = sql.trim().to_uppercase();
        let has_returning = upper.contains(" RETURNING ");

        // Handle catalog queries directly (no need to prepare)
        if let Some(catalog_query) = catalog::intercept_catalog_query(sql) {
            let result = catalog::get_dynamic_catalog_result(catalog_query, backend);
            let fields: Vec<FieldDescription> = result.columns.iter().map(|name| {
                FieldDescription {
                    name: name.clone(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: OID_TEXT,
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                }
            }).collect();
            if !fields.is_empty() {
                let row_desc = BackendMessage::RowDescription { fields };
                stream.write_all(&row_desc.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                return Ok(());
            }
        }

        // Handle EXPLAIN directly - return a single "QUERY PLAN" text column
        if upper.starts_with("EXPLAIN ") {
            let fields = vec![FieldDescription {
                name: "QUERY PLAN".to_string(),
                table_oid: 0,
                column_attr: 0,
                type_oid: OID_TEXT,
                type_size: -1,
                type_modifier: -1,
                format: 0,
            }];
            let row_desc = BackendMessage::RowDescription { fields };
            stream.write_all(&row_desc.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
            return Ok(());
        }

        // Handle SHOW commands directly (no need to prepare)
        if upper.starts_with("SHOW") {
            if let Some(result) = handle_show_command(sql) {
                let fields: Vec<FieldDescription> = result.columns.iter().map(|c| {
                    FieldDescription {
                        name: c.name.clone(),
                        table_oid: 0,
                        column_attr: 0,
                        type_oid: c.type_oid,
                        type_size: -1,
                        type_modifier: -1,
                        format: 0,
                    }
                }).collect();
                let row_desc = BackendMessage::RowDescription { fields };
                stream.write_all(&row_desc.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                return Ok(());
            }
        }

        if upper.starts_with("SELECT") || upper.starts_with("WITH")
            || has_returning {
            // Try to describe the query columns using prepare (without executing)
            match backend.describe_query(sql) {
                Ok(columns) if !columns.is_empty() => {
                    let fields: Vec<FieldDescription> = columns.iter().map(|c| {
                        FieldDescription {
                            name: c.name.clone(),
                            table_oid: 0,
                            column_attr: 0,
                            type_oid: c.type_oid,
                            type_size: -1,
                            type_modifier: -1,
                            format: 0,
                        }
                    }).collect();
                    let row_desc = BackendMessage::RowDescription { fields };
                    stream.write_all(&row_desc.serialize()).await
                        .map_err(|e| format!("Write error: {}", e))?;
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    // No columns / not a SELECT
    let resp = BackendMessage::NoData;
    stream.write_all(&resp.serialize()).await
        .map_err(|e| format!("Write error: {}", e))
}

/// Detect transaction control commands and return the new state
fn detect_transaction_command(sql: &str, _current: TransactionState) -> Option<TransactionState> {
    let upper = sql.trim().to_uppercase();
    if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
        Some(TransactionState::InTransaction)
    } else if upper.starts_with("SAVEPOINT ") || upper.starts_with("RELEASE SAVEPOINT ")
        || upper.starts_with("RELEASE ") || upper.starts_with("ROLLBACK TO ") {
        // Savepoints don't change the top-level transaction state
        None
    } else if upper.starts_with("COMMIT") || upper.starts_with("END") || upper.starts_with("ROLLBACK") {
        Some(TransactionState::Idle)
    } else {
        None
    }
}

async fn handle_execute(
    stream: &mut TcpStream,
    backend: &SqliteBackend,
    state: &mut ConnectionState,
    portal_name: &str,
    _max_rows: i32,
) -> Result<(), String> {
    let portal = state.portals.get(portal_name);
    let (query, params) = match portal {
        Some(p) => (p.query.as_str(), &p.params),
        None => ("", &vec![] as &Vec<Option<Vec<u8>>>),
    };

    if query.is_empty() {
        let resp = BackendMessage::EmptyQueryResponse;
        stream.write_all(&resp.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;
        return Ok(());
    }

    // Handle EXPLAIN in extended query protocol
    let upper_q = query.trim().to_uppercase();
    if upper_q.starts_with("EXPLAIN ") {
        return match handle_explain(backend, query) {
            Ok(result) => {
                for row in &result.rows {
                    let values: Vec<Option<Vec<u8>>> = row.iter()
                        .map(|v| v.as_ref().map(|s| s.as_bytes().to_vec()))
                        .collect();
                    let data_row = BackendMessage::DataRow { values };
                    stream.write_all(&data_row.serialize()).await
                        .map_err(|e| format!("Write error: {}", e))?;
                }
                let complete = BackendMessage::CommandComplete {
                    tag: result.command_tag,
                };
                stream.write_all(&complete.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                Ok(())
            }
            Err(err) => {
                let error = BackendMessage::ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: sqlite_error_to_sqlstate(&err).to_string(),
                    message: err,
                };
                stream.write_all(&error.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                Ok(())
            }
        };
    }

    // Use the unified catalog/SHOW handler first, then execute with params
    let result = if let Some(result) = handle_show_command(query) {
        Ok(result)
    } else if let Some(catalog_query) = catalog::intercept_catalog_query(query) {
        let cr = catalog::get_dynamic_catalog_result(catalog_query, backend);
        Ok(crate::backend::sqlite::QueryResult {
            columns: cr.columns.iter().map(|name| crate::backend::sqlite::ColumnInfo {
                name: name.clone(),
                type_oid: crate::backend::sqlite::OID_TEXT,
            }).collect(),
            rows: cr.rows,
            rows_affected: 0,
            command_tag: cr.tag,
        })
    } else if params.is_empty() {
        backend.execute(query)
    } else {
        backend.execute_with_params(query, params)
    };

    match result {
        Ok(result) => {
            // Track transaction state transitions
            if let Some(new_state) = detect_transaction_command(query, state.transaction_state) {
                state.transaction_state = new_state;
            }

            // Send DataRows (no RowDescription here - it was sent in Describe)
            for row in &result.rows {
                let values: Vec<Option<Vec<u8>>> = row.iter()
                    .map(|v| v.as_ref().map(|s| s.as_bytes().to_vec()))
                    .collect();
                let data_row = BackendMessage::DataRow { values };
                stream.write_all(&data_row.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
            }

            let complete = BackendMessage::CommandComplete {
                tag: result.command_tag,
            };
            stream.write_all(&complete.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
        }
        Err(err) => {
            // If in a transaction, move to Failed state
            if state.transaction_state == TransactionState::InTransaction {
                state.transaction_state = TransactionState::Failed;
            }

            let error = BackendMessage::ErrorResponse {
                severity: "ERROR".to_string(),
                code: sqlite_error_to_sqlstate(&err).to_string(),
                message: err,
            };
            stream.write_all(&error.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
        }
    }
    Ok(())
}

use crate::backend::sqlite::{QueryResult, ColumnInfo, OID_TEXT};

fn execute_with_catalog(backend: &SqliteBackend, sql: &str) -> Result<QueryResult, String> {
    // Handle SHOW commands
    if let Some(result) = handle_show_command(sql) {
        return Ok(result);
    }

    // Handle EXPLAIN
    let upper = sql.trim().to_uppercase();
    if upper.starts_with("EXPLAIN ") {
        return handle_explain(backend, sql);
    }

    // Handle LISTEN/NOTIFY as no-ops
    if upper.starts_with("LISTEN") || upper.starts_with("UNLISTEN") {
        return Ok(QueryResult {
            columns: vec![], rows: vec![], rows_affected: 0,
            command_tag: "LISTEN".to_string(),
        });
    }
    if upper.starts_with("NOTIFY") {
        return Ok(QueryResult {
            columns: vec![], rows: vec![], rows_affected: 0,
            command_tag: "NOTIFY".to_string(),
        });
    }

    // Handle DISCARD ALL / RESET ALL as no-ops (connection pool commands)
    if upper == "DISCARD ALL" || upper == "RESET ALL" || upper.starts_with("DEALLOCATE ALL") {
        let tag = if upper == "DISCARD ALL" { "DISCARD ALL" }
            else if upper == "RESET ALL" { "RESET ALL" }
            else { "DEALLOCATE ALL" };
        return Ok(QueryResult {
            columns: vec![], rows: vec![], rows_affected: 0,
            command_tag: tag.to_string(),
        });
    }

    // Check for catalog queries
    if let Some(catalog_query) = catalog::intercept_catalog_query(sql) {
        let result = catalog::get_dynamic_catalog_result(catalog_query, backend);
        return Ok(QueryResult {
            columns: result.columns.iter().map(|name| ColumnInfo {
                name: name.clone(),
                type_oid: OID_TEXT,
            }).collect(),
            rows: result.rows,
            rows_affected: 0,
            command_tag: result.tag,
        });
    }
    backend.execute(sql)
}

async fn handle_startup(stream: &mut TcpStream) -> Result<(), String> {
    loop {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await
            .map_err(|e| format!("Read error: {}", e))?;
        let length = i32::from_be_bytes(len_buf) as usize;

        if length > MAX_MESSAGE_SIZE {
            return Err(format!("Startup message too large: {} bytes (max {})", length, MAX_MESSAGE_SIZE));
        }
        if length < 4 {
            return Err("Invalid startup message length".to_string());
        }

        let mut body = vec![0u8; length - 4];
        stream.read_exact(&mut body).await
            .map_err(|e| format!("Read error: {}", e))?;

        let mut full = Vec::with_capacity(length);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);

        let msg = FrontendMessage::parse_startup(&Bytes::from(full))
            .map_err(|e| format!("Startup parse error: {}", e))?;

        match msg {
            FrontendMessage::SslRequest => {
                stream.write_all(b"N").await
                    .map_err(|e| format!("Write error: {}", e))?;
                continue;
            }
            FrontendMessage::Startup { version, params } => {
                tracing::info!(
                    "Client startup: version={}, user={:?}, database={:?}",
                    version,
                    params.get("user"),
                    params.get("database")
                );
                return Ok(());
            }
            _ => return Err("Unexpected startup message".to_string()),
        }
    }
}

async fn send_startup_response(stream: &mut TcpStream) -> Result<(), String> {
    let messages: Vec<BackendMessage> = vec![
        BackendMessage::AuthenticationOk,
        BackendMessage::ParameterStatus {
            name: "server_version".to_string(),
            value: "15.0.0".to_string(),
        },
        BackendMessage::ParameterStatus {
            name: "server_encoding".to_string(),
            value: "UTF8".to_string(),
        },
        BackendMessage::ParameterStatus {
            name: "client_encoding".to_string(),
            value: "UTF8".to_string(),
        },
        BackendMessage::ParameterStatus {
            name: "DateStyle".to_string(),
            value: "ISO, MDY".to_string(),
        },
        BackendMessage::ParameterStatus {
            name: "integer_datetimes".to_string(),
            value: "on".to_string(),
        },
        BackendMessage::ParameterStatus {
            name: "standard_conforming_strings".to_string(),
            value: "on".to_string(),
        },
        BackendMessage::BackendKeyData {
            process_id: std::process::id() as i32,
            secret_key: 0,
        },
        BackendMessage::ReadyForQuery { status: b'I' },
    ];

    for msg in messages {
        stream.write_all(&msg.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;
    }
    Ok(())
}

/// Parse a COPY FROM STDIN command, returning (table_name, columns, delimiter)
fn parse_copy_from_stdin(sql: &str) -> Option<(String, Vec<String>, char)> {
    let upper = sql.trim().to_uppercase();
    if !upper.starts_with("COPY ") || !upper.contains("FROM STDIN") {
        return None;
    }

    let trimmed = sql.trim();
    // Extract table name (after COPY, before ( or FROM)
    let after_copy = &trimmed[5..].trim_start();
    let table_end = after_copy.find(|c: char| c == '(' || c.is_whitespace()).unwrap_or(after_copy.len());
    let table = after_copy[..table_end].trim().to_string();

    // Extract columns if present
    let mut columns = Vec::new();
    if let Some(paren_start) = after_copy.find('(') {
        if let Some(paren_end) = after_copy.find(')') {
            let cols_str = &after_copy[paren_start + 1..paren_end];
            columns = cols_str.split(',')
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect();
        }
    }

    // Extract delimiter from WITH options
    let mut delimiter = '\t'; // default is tab
    if upper.contains("CSV") || upper.contains("DELIMITER") {
        delimiter = ','; // default for CSV
    }
    // Look for DELIMITER 'X' pattern
    if let Some(delim_pos) = upper.find("DELIMITER") {
        let after_delim = &trimmed[delim_pos + 9..];
        if let Some(quote_start) = after_delim.find('\'') {
            if let Some(ch) = after_delim[quote_start + 1..].chars().next() {
                delimiter = ch;
            }
        }
    }

    Some((table, columns, delimiter))
}

/// Handle the COPY FROM STDIN sub-protocol
async fn handle_copy_from_stdin(
    stream: &mut TcpStream,
    backend: &SqliteBackend,
    table: &str,
    columns: &[String],
    delimiter: char,
) -> Result<(), String> {
    // Determine column count for CopyInResponse
    let num_cols = if columns.is_empty() {
        // Query table info to get column count
        let conn_tables = backend.query_sqlite_master_tables();
        if conn_tables.iter().any(|t| t.eq_ignore_ascii_case(table)) {
            let all_cols = backend.query_sqlite_master_columns();
            all_cols.iter().filter(|(t, _, _, _, _)| t.eq_ignore_ascii_case(table)).count()
        } else {
            1 // fallback
        }
    } else {
        columns.len()
    };

    // Send CopyInResponse
    let copy_in = BackendMessage::CopyInResponse {
        format: 0, // text format
        column_formats: vec![0i16; num_cols],
    };
    stream.write_all(&copy_in.serialize()).await
        .map_err(|e| format!("Write error: {}", e))?;

    // Collect data from CopyData messages
    let mut data_buf = Vec::new();

    loop {
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await
            .map_err(|e| format!("COPY read error: {}", e))?;

        let msg_type = header[0];
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let body_len = length.saturating_sub(4);
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            stream.read_exact(&mut body).await
                .map_err(|e| format!("COPY read body error: {}", e))?;
        }

        match msg_type {
            b'd' => {
                // CopyData
                data_buf.extend_from_slice(&body);
            }
            b'c' => {
                // CopyDone - parse and insert data
                break;
            }
            b'f' => {
                // CopyFail - client is aborting
                let msg = String::from_utf8_lossy(&body).trim_end_matches('\0').to_string();
                let error = BackendMessage::ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: "57014".to_string(),
                    message: format!("COPY failed: {}", msg),
                };
                stream.write_all(&error.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                return Ok(());
            }
            _ => {
                return Err(format!("Unexpected message type during COPY: {}", msg_type as char));
            }
        }
    }

    // Parse the accumulated data into rows
    let data_str = String::from_utf8(data_buf)
        .map_err(|e| format!("COPY data encoding error: {}", e))?;

    let rows: Vec<Vec<String>> = data_str.lines()
        .filter(|line| !line.is_empty() && *line != "\\.")
        .map(|line| {
            line.split(delimiter)
                .map(|field| field.to_string())
                .collect()
        })
        .collect();

    match backend.copy_from_stdin(table, columns, &rows, delimiter) {
        Ok(count) => {
            let complete = BackendMessage::CommandComplete {
                tag: format!("COPY {}", count),
            };
            stream.write_all(&complete.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
        }
        Err(e) => {
            let error = BackendMessage::ErrorResponse {
                severity: "ERROR".to_string(),
                code: sqlite_error_to_sqlstate(&e).to_string(),
                message: e,
            };
            stream.write_all(&error.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
        }
    }

    Ok(())
}

async fn handle_simple_query(
    stream: &mut TcpStream,
    backend: &SqliteBackend,
    sql: &str,
    state: &mut ConnectionState,
) -> Result<(), String> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        let resp = BackendMessage::EmptyQueryResponse;
        stream.write_all(&resp.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;
        let ready = BackendMessage::ReadyForQuery {
            status: state.transaction_state.status_byte(),
        };
        stream.write_all(&ready.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;
        return Ok(());
    }

    // Handle COPY FROM STDIN
    if let Some((table, columns, delimiter)) = parse_copy_from_stdin(trimmed) {
        handle_copy_from_stdin(stream, backend, &table, &columns, delimiter).await?;
        let ready = BackendMessage::ReadyForQuery {
            status: state.transaction_state.status_byte(),
        };
        stream.write_all(&ready.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;
        return Ok(());
    }

    // Use sqlparser to properly split statements (handles semicolons in strings)
    let dialect = PostgreSqlDialect {};
    // If parsing fails, fall back to treating the whole thing as one statement
    // This handles edge cases like SHOW commands that sqlparser may not parse
    let parsed_statements = Parser::parse_sql(&dialect, trimmed).unwrap_or_default();

    // Convert parsed AST back to strings, or fall back to raw SQL
    let statement_strings: Vec<String> = if parsed_statements.is_empty() {
        vec![trimmed.to_string()]
    } else {
        parsed_statements.iter().map(|s| s.to_string()).collect()
    };

    if statement_strings.is_empty() {
        let resp = BackendMessage::EmptyQueryResponse;
        stream.write_all(&resp.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;
    }

    for stmt_sql in &statement_strings {
        let stmt_sql = stmt_sql.as_str();
        // Handle SHOW commands inline
        if let Some(show_result) = handle_show_command(stmt_sql) {
            send_query_result(stream, &show_result).await?;
            continue;
        }

        // Handle LISTEN/NOTIFY as no-ops
        let upper = stmt_sql.to_uppercase();
        if upper.starts_with("LISTEN") || upper.starts_with("UNLISTEN") {
            let complete = BackendMessage::CommandComplete {
                tag: "LISTEN".to_string(),
            };
            stream.write_all(&complete.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
            continue;
        }
        if upper.starts_with("NOTIFY") {
            let complete = BackendMessage::CommandComplete {
                tag: "NOTIFY".to_string(),
            };
            stream.write_all(&complete.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
            continue;
        }

        // Handle DISCARD ALL / RESET ALL as no-ops (used by connection pools)
        if upper == "DISCARD ALL" || upper == "RESET ALL" || upper.starts_with("DEALLOCATE ALL") {
            let tag = if upper == "DISCARD ALL" { "DISCARD ALL" }
                else if upper == "RESET ALL" { "RESET ALL" }
                else { "DEALLOCATE ALL" };
            let complete = BackendMessage::CommandComplete {
                tag: tag.to_string(),
            };
            stream.write_all(&complete.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
            continue;
        }

        // Handle SQL-level PREPARE name AS query
        if upper.starts_with("PREPARE ") {
            if let Some(result) = handle_sql_prepare(stmt_sql, &mut state.sql_prepared) {
                let complete = BackendMessage::CommandComplete {
                    tag: result,
                };
                stream.write_all(&complete.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                continue;
            }
        }

        // Handle SQL-level EXECUTE name(params)
        if upper.starts_with("EXECUTE ") {
            if let Some(resolved) = handle_sql_execute(stmt_sql, &state.sql_prepared) {
                match execute_with_catalog(backend, &resolved) {
                    Ok(result) => {
                        if let Some(new_state) = detect_transaction_command(&resolved, state.transaction_state) {
                            state.transaction_state = new_state;
                        }
                        send_query_result(stream, &result).await?;
                    }
                    Err(err) => {
                        if state.transaction_state == TransactionState::InTransaction {
                            state.transaction_state = TransactionState::Failed;
                        }
                        let error = BackendMessage::ErrorResponse {
                            severity: "ERROR".to_string(),
                            code: sqlite_error_to_sqlstate(&err).to_string(),
                            message: err,
                        };
                        stream.write_all(&error.serialize()).await
                            .map_err(|e| format!("Write error: {}", e))?;
                        break;
                    }
                }
                continue;
            }
        }

        // Handle SQL-level DEALLOCATE name
        if upper.starts_with("DEALLOCATE ") && upper != "DEALLOCATE ALL" {
            let name = upper.trim_start_matches("DEALLOCATE").trim().trim_start_matches("PREPARE").trim();
            state.sql_prepared.remove(name);
            let complete = BackendMessage::CommandComplete {
                tag: "DEALLOCATE".to_string(),
            };
            stream.write_all(&complete.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
            continue;
        }

        match execute_with_catalog(backend, stmt_sql) {
            Ok(result) => {
                // Track transaction state transitions
                if let Some(new_state) = detect_transaction_command(stmt_sql, state.transaction_state) {
                    state.transaction_state = new_state;
                }

                send_query_result(stream, &result).await?;
            }
            Err(err) => {
                if state.transaction_state == TransactionState::InTransaction {
                    state.transaction_state = TransactionState::Failed;
                }

                let error = BackendMessage::ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: sqlite_error_to_sqlstate(&err).to_string(),
                    message: err,
                };
                stream.write_all(&error.serialize()).await
                    .map_err(|e| format!("Write error: {}", e))?;
                // After error, stop processing remaining statements
                break;
            }
        }
    }

    let ready = BackendMessage::ReadyForQuery {
        status: state.transaction_state.status_byte(),
    };
    stream.write_all(&ready.serialize()).await
        .map_err(|e| format!("Write error: {}", e))?;
    Ok(())
}

async fn send_query_result(
    stream: &mut TcpStream,
    result: &QueryResult,
) -> Result<(), String> {
    if !result.columns.is_empty() {
        let fields: Vec<FieldDescription> = result.columns.iter().map(|c| {
            FieldDescription {
                name: c.name.clone(),
                table_oid: 0,
                column_attr: 0,
                type_oid: c.type_oid,
                type_size: -1,
                type_modifier: -1,
                format: 0,
            }
        }).collect();
        let row_desc = BackendMessage::RowDescription { fields };
        stream.write_all(&row_desc.serialize()).await
            .map_err(|e| format!("Write error: {}", e))?;

        for row in &result.rows {
            let values: Vec<Option<Vec<u8>>> = row.iter()
                .map(|v| v.as_ref().map(|s| s.as_bytes().to_vec()))
                .collect();
            let data_row = BackendMessage::DataRow { values };
            stream.write_all(&data_row.serialize()).await
                .map_err(|e| format!("Write error: {}", e))?;
        }
    }

    let complete = BackendMessage::CommandComplete {
        tag: result.command_tag.clone(),
    };
    stream.write_all(&complete.serialize()).await
        .map_err(|e| format!("Write error: {}", e))
}

/// Handle EXPLAIN and EXPLAIN ANALYZE queries
fn handle_explain(backend: &SqliteBackend, sql: &str) -> Result<QueryResult, String> {
    let upper = sql.trim().to_uppercase();

    // Determine if it's EXPLAIN ANALYZE or plain EXPLAIN
    let (is_analyze, inner_sql) = if upper.starts_with("EXPLAIN ANALYZE ") {
        (true, sql.trim()[16..].trim())
    } else if upper.starts_with("EXPLAIN VERBOSE ") {
        (false, sql.trim()[16..].trim())
    } else if upper.starts_with("EXPLAIN (ANALYZE) ") {
        (true, sql.trim()[18..].trim())
    } else if upper.starts_with("EXPLAIN (ANALYZE, VERBOSE) ") {
        (true, sql.trim()[27..].trim())
    } else {
        (false, sql.trim()[8..].trim())
    };

    // Translate the inner SQL to SQLite dialect
    let translated = crate::sql::translator::Translator::translate(inner_sql)?;

    // Get the query plan using EXPLAIN QUERY PLAN (execute raw - no translation)
    let plan_sql = format!("EXPLAIN QUERY PLAN {}", translated);
    let plan_result = backend.execute_raw(&plan_sql);

    let mut rows = Vec::new();

    // If ANALYZE, also execute the query and measure time
    if is_analyze {
        let start = std::time::Instant::now();
        let exec_result = backend.execute(inner_sql);
        let elapsed = start.elapsed();

        // Add plan lines
        if let Ok(ref plan) = plan_result {
            for row in &plan.rows {
                let line = row.iter()
                    .filter_map(|v| v.as_ref())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" | ");
                rows.push(vec![Some(line)]);
            }
        }

        // Add execution stats
        match exec_result {
            Ok(result) => {
                rows.push(vec![Some(format!(
                    "Execution Time: {:.3} ms (rows: {})",
                    elapsed.as_secs_f64() * 1000.0,
                    result.rows.len()
                ))]);
            }
            Err(e) => {
                rows.push(vec![Some(format!("Execution Error: {}", e))]);
            }
        }
    } else {
        // Plain EXPLAIN - just show the plan
        if let Ok(ref plan) = plan_result {
            for row in &plan.rows {
                let line = row.iter()
                    .filter_map(|v| v.as_ref())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" | ");
                rows.push(vec![Some(line)]);
            }
        } else if let Err(e) = plan_result {
            rows.push(vec![Some(format!("Plan Error: {}", e))]);
        }
    }

    let count = rows.len();
    Ok(QueryResult {
        columns: vec![ColumnInfo {
            name: "QUERY PLAN".to_string(),
            type_oid: OID_TEXT,
        }],
        rows,
        rows_affected: 0,
        command_tag: format!("EXPLAIN {}", count),
    })
}

/// Handle SQL-level PREPARE name [(types)] AS query
fn handle_sql_prepare(sql: &str, prepared: &mut HashMap<String, String>) -> Option<String> {
    let upper = sql.trim().to_uppercase();
    if !upper.starts_with("PREPARE ") {
        return None;
    }

    let rest = sql.trim()[8..].trim(); // after "PREPARE "

    // Find "AS" keyword
    let upper_rest = rest.to_uppercase();
    let as_pos = upper_rest.find(" AS ")?;
    let name_part = rest[..as_pos].trim();
    let query = rest[as_pos + 4..].trim();

    // name might be "name(type, type)" or just "name"
    let name = if let Some(paren) = name_part.find('(') {
        name_part[..paren].trim()
    } else {
        name_part
    };

    prepared.insert(name.to_uppercase(), query.to_string());
    Some("PREPARE".to_string())
}

/// Handle SQL-level EXECUTE name [(params)]
fn handle_sql_execute(sql: &str, prepared: &HashMap<String, String>) -> Option<String> {
    let upper = sql.trim().to_uppercase();
    if !upper.starts_with("EXECUTE ") {
        return None;
    }

    let rest = sql.trim()[8..].trim();

    // Extract name and optional params
    let (name, params) = if let Some(paren) = rest.find('(') {
        let name = rest[..paren].trim();
        let params_str = &rest[paren + 1..];
        let params_str = params_str.trim_end_matches(')').trim();
        let params: Vec<&str> = if params_str.is_empty() {
            vec![]
        } else {
            params_str.split(',').map(|p| p.trim()).collect()
        };
        (name, params)
    } else {
        (rest.trim_end_matches(';'), vec![])
    };

    let query = prepared.get(&name.to_uppercase())?;

    // Replace $1, $2, etc. with actual parameter values
    let mut result = query.clone();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("${}", i + 1);
        result = result.replace(&placeholder, param);
    }

    Some(result)
}

/// Handle SHOW commands that ORMs send
fn handle_show_command(sql: &str) -> Option<QueryResult> {
    let upper = sql.trim().to_uppercase();
    if !upper.starts_with("SHOW ") {
        return None;
    }

    let setting = upper.trim_start_matches("SHOW ").trim().trim_end_matches(';');
    let (col_name, value) = match setting {
        "TRANSACTION ISOLATION LEVEL" => ("transaction_isolation", "read committed"),
        "SERVER_VERSION" | "SERVER VERSION" => ("server_version", "15.0.0"),
        "SERVER_VERSION_NUM" => ("server_version_num", "150000"),
        "SERVER_ENCODING" | "SERVER ENCODING" => ("server_encoding", "UTF8"),
        "CLIENT_ENCODING" | "CLIENT ENCODING" => ("client_encoding", "UTF8"),
        "STANDARD_CONFORMING_STRINGS" => ("standard_conforming_strings", "on"),
        "DATESTYLE" | "DATE STYLE" => ("DateStyle", "ISO, MDY"),
        "TIMEZONE" | "TIME ZONE" => ("TimeZone", "UTC"),
        "INTEGER_DATETIMES" => ("integer_datetimes", "on"),
        "LC_COLLATE" => ("lc_collate", "en_US.UTF-8"),
        "LC_CTYPE" => ("lc_ctype", "en_US.UTF-8"),
        "MAX_CONNECTIONS" => ("max_connections", "100"),
        "SEARCH_PATH" | "SEARCH PATH" => ("search_path", "\"$user\", public"),
        _ => (setting, ""),
    };

    Some(QueryResult {
        columns: vec![ColumnInfo {
            name: col_name.to_lowercase(),
            type_oid: OID_TEXT,
        }],
        rows: vec![vec![Some(value.to_string())]],
        rows_affected: 0,
        command_tag: "SHOW".to_string(),
    })
}

/// Map SQLite error messages to PostgreSQL SQLSTATE codes.
///
/// SQLSTATE codes:
/// - 23505: unique_violation
/// - 23503: foreign_key_violation
/// - 23502: not_null_violation
/// - 23514: check_violation
/// - 42P01: undefined_table
/// - 42703: undefined_column
/// - 42601: syntax_error
/// - 42P07: duplicate_table
/// - 42710: duplicate_object
/// - 40001: serialization_failure
/// - 22P02: invalid_text_representation (type errors)
/// - XX000: internal_error (fallback)
fn sqlite_error_to_sqlstate(error_msg: &str) -> &'static str {
    let msg = error_msg.to_uppercase();

    // Unique constraint violation
    if msg.contains("UNIQUE CONSTRAINT") || msg.contains("UNIQUE") && msg.contains("FAILED") {
        return "23505";
    }

    // Foreign key violation
    if msg.contains("FOREIGN KEY CONSTRAINT") || msg.contains("FOREIGN KEY") {
        return "23503";
    }

    // NOT NULL violation
    if msg.contains("NOT NULL CONSTRAINT") || (msg.contains("NOT NULL") && msg.contains("FAILED")) {
        return "23502";
    }

    // CHECK constraint violation
    if msg.contains("CHECK CONSTRAINT") {
        return "23514";
    }

    // Table not found
    if msg.contains("NO SUCH TABLE") || msg.contains("TABLE") && msg.contains("NOT FOUND") {
        return "42P01";
    }

    // Column not found
    if msg.contains("NO SUCH COLUMN") || msg.contains("HAS NO COLUMN") {
        return "42703";
    }

    // Table already exists
    if msg.contains("TABLE") && msg.contains("ALREADY EXISTS") {
        return "42P07";
    }

    // Index already exists
    if msg.contains("INDEX") && msg.contains("ALREADY EXISTS") {
        return "42710";
    }

    // Syntax error
    if msg.contains("SYNTAX ERROR") || msg.contains("NEAR \"") {
        return "42601";
    }

    // Parse error from translator
    if msg.contains("PARSE ERROR") {
        return "42601";
    }

    // Database locked / busy
    if msg.contains("DATABASE IS LOCKED") || msg.contains("BUSY") {
        return "40001";
    }

    // Type mismatch
    if msg.contains("DATATYPE MISMATCH") {
        return "22P02";
    }

    // Default: internal error
    "XX000"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use bytes::{BufMut, BytesMut};

    async fn start_test_server() -> u16 {
        let backend = SqliteBackend::new(":memory:").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let backend = Arc::new(backend);

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let backend = backend.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(stream, backend).await;
                });
            }
        });

        port
    }

    async fn send_startup(stream: &mut TcpStream) {
        let mut buf = BytesMut::new();
        let mut body = BytesMut::new();
        body.put_i32(196608);
        body.put_slice(b"user\0testuser\0database\0testdb\0\0");
        buf.put_i32((body.len() + 4) as i32);
        buf.extend_from_slice(&body);
        stream.write_all(&buf).await.unwrap();
    }

    async fn read_until_ready(stream: &mut TcpStream) -> Vec<u8> {
        let mut all = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            all.extend_from_slice(&buf[..n]);
            if all.len() >= 6 {
                let tail = &all[all.len() - 6..];
                if tail[0] == b'Z' {
                    break;
                }
            }
        }
        all
    }

    async fn send_query(stream: &mut TcpStream, sql: &str) {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((4 + sql.len() + 1) as i32);
        buf.put_slice(sql.as_bytes());
        buf.put_u8(0);
        stream.write_all(&buf).await.unwrap();
    }

    #[tokio::test]
    async fn test_server_accepts_connection() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'R'));
        assert!(response.contains(&b'Z'));
    }

    #[tokio::test]
    async fn test_server_handles_simple_query() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        send_query(&mut stream, "SELECT 1").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'T'));
        assert!(response.contains(&b'D'));
        assert!(response.contains(&b'C'));
        assert!(response.contains(&b'Z'));
    }

    #[tokio::test]
    async fn test_server_handles_create_and_insert() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        send_query(&mut stream, "CREATE TABLE test (id INTEGER, name TEXT)").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'C'));

        send_query(&mut stream, "INSERT INTO test VALUES (1, 'hello')").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'C'));

        send_query(&mut stream, "SELECT * FROM test").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'D'));
    }

    #[tokio::test]
    async fn test_server_handles_error() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        send_query(&mut stream, "SELECT * FROM nonexistent").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'E'));
        assert!(response.contains(&b'Z'));
    }

    #[tokio::test]
    async fn test_server_ssl_rejection() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(80877103);
        stream.write_all(&buf).await.unwrap();

        let mut resp = [0u8; 1];
        stream.read_exact(&mut resp).await.unwrap();
        assert_eq!(resp[0], b'N');

        send_startup(&mut stream).await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'R'));
    }

    #[tokio::test]
    async fn test_server_terminate() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        let mut buf = BytesMut::new();
        buf.put_u8(b'X');
        buf.put_i32(4);
        stream.write_all(&buf).await.unwrap();

        let mut resp = [0u8; 1];
        let result = stream.read(&mut resp).await.unwrap();
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_server_pg_types_roundtrip() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        send_query(&mut stream,
            "CREATE TABLE users (id UUID PRIMARY KEY, name VARCHAR(255), active BOOLEAN)"
        ).await;
        let _ = read_until_ready(&mut stream).await;

        send_query(&mut stream,
            "INSERT INTO users VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice', 'true')"
        ).await;
        let _ = read_until_ready(&mut stream).await;

        send_query(&mut stream, "SELECT * FROM users").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'D'));
    }

    #[tokio::test]
    async fn test_oversized_message_rejected() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        // Send a Query message with a fake length exceeding MAX_MESSAGE_SIZE
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        // Length field: 200MB (exceeds 100MB max)
        buf.put_i32(200 * 1024 * 1024);
        stream.write_all(&buf).await.unwrap();

        // Server should respond with ErrorResponse (byte 'E') and close connection
        let mut response = vec![0u8; 512];
        let n = stream.read(&mut response).await.unwrap_or(0);
        if n > 0 {
            assert_eq!(response[0], b'E', "Expected ErrorResponse for oversized message");
        }
        // Connection should be closed after fatal error
    }

    #[tokio::test]
    async fn test_max_message_size_constant() {
        assert_eq!(MAX_MESSAGE_SIZE, 100 * 1024 * 1024);
    }

    #[test]
    fn test_sql_prepare_simple() {
        let mut prepared = HashMap::new();
        let result = handle_sql_prepare("PREPARE myplan AS SELECT * FROM users WHERE id = $1", &mut prepared);
        assert_eq!(result, Some("PREPARE".to_string()));
        assert!(prepared.contains_key("MYPLAN"));
        assert_eq!(prepared["MYPLAN"], "SELECT * FROM users WHERE id = $1");
    }

    #[test]
    fn test_sql_prepare_with_types() {
        let mut prepared = HashMap::new();
        let result = handle_sql_prepare(
            "PREPARE myplan (int, text) AS SELECT * FROM users WHERE id = $1 AND name = $2",
            &mut prepared
        );
        assert_eq!(result, Some("PREPARE".to_string()));
        assert!(prepared.contains_key("MYPLAN"));
    }

    #[test]
    fn test_sql_execute_simple() {
        let mut prepared = HashMap::new();
        prepared.insert("MYPLAN".to_string(), "SELECT * FROM users".to_string());
        let result = handle_sql_execute("EXECUTE myplan", &prepared);
        assert_eq!(result, Some("SELECT * FROM users".to_string()));
    }

    #[test]
    fn test_sql_execute_with_params() {
        let mut prepared = HashMap::new();
        prepared.insert("MYPLAN".to_string(), "SELECT * FROM users WHERE id = $1 AND name = $2".to_string());
        let result = handle_sql_execute("EXECUTE myplan(42, 'alice')", &prepared);
        assert_eq!(result, Some("SELECT * FROM users WHERE id = 42 AND name = 'alice'".to_string()));
    }

    #[test]
    fn test_sql_execute_unknown_plan() {
        let prepared = HashMap::new();
        let result = handle_sql_execute("EXECUTE unknown_plan", &prepared);
        assert!(result.is_none());
    }

    #[test]
    fn test_sqlstate_unique_violation() {
        assert_eq!(sqlite_error_to_sqlstate("UNIQUE constraint failed: users.email"), "23505");
    }

    #[test]
    fn test_sqlstate_foreign_key_violation() {
        assert_eq!(sqlite_error_to_sqlstate("FOREIGN KEY constraint failed"), "23503");
    }

    #[test]
    fn test_sqlstate_not_null_violation() {
        assert_eq!(sqlite_error_to_sqlstate("NOT NULL constraint failed: users.name"), "23502");
    }

    #[test]
    fn test_sqlstate_check_violation() {
        assert_eq!(sqlite_error_to_sqlstate("CHECK constraint failed: age_positive"), "23514");
    }

    #[test]
    fn test_sqlstate_table_not_found() {
        assert_eq!(sqlite_error_to_sqlstate("Execute error: no such table: nonexistent"), "42P01");
    }

    #[test]
    fn test_sqlstate_column_not_found() {
        assert_eq!(sqlite_error_to_sqlstate("Execute error: no such column: foo"), "42703");
    }

    #[test]
    fn test_sqlstate_table_already_exists() {
        assert_eq!(sqlite_error_to_sqlstate("Execute error: table users already exists"), "42P07");
    }

    #[test]
    fn test_sqlstate_syntax_error() {
        assert_eq!(sqlite_error_to_sqlstate("near \"SELEC\": syntax error"), "42601");
    }

    #[test]
    fn test_sqlstate_parse_error() {
        assert_eq!(sqlite_error_to_sqlstate("Parse error: Expected SELECT"), "42601");
    }

    #[test]
    fn test_sqlstate_database_locked() {
        assert_eq!(sqlite_error_to_sqlstate("database is locked"), "40001");
    }

    #[test]
    fn test_sqlstate_unknown_error() {
        assert_eq!(sqlite_error_to_sqlstate("something went wrong"), "XX000");
    }

    #[test]
    fn test_parse_copy_from_stdin_basic() {
        let result = parse_copy_from_stdin("COPY mytable FROM STDIN");
        assert!(result.is_some());
        let (table, columns, delimiter) = result.unwrap();
        assert_eq!(table, "mytable");
        assert!(columns.is_empty());
        assert_eq!(delimiter, '\t'); // default tab delimiter
    }

    #[test]
    fn test_parse_copy_from_stdin_with_columns() {
        let result = parse_copy_from_stdin("COPY mytable (id, name, value) FROM STDIN");
        assert!(result.is_some());
        let (table, columns, _) = result.unwrap();
        assert_eq!(table, "mytable");
        assert_eq!(columns, vec!["id", "name", "value"]);
    }

    #[test]
    fn test_parse_copy_from_stdin_csv() {
        let result = parse_copy_from_stdin("COPY mytable FROM STDIN WITH CSV");
        assert!(result.is_some());
        let (_, _, delimiter) = result.unwrap();
        assert_eq!(delimiter, ',');
    }

    #[test]
    fn test_parse_copy_from_stdin_custom_delimiter() {
        let result = parse_copy_from_stdin("COPY mytable FROM STDIN WITH DELIMITER '|'");
        assert!(result.is_some());
        let (_, _, delimiter) = result.unwrap();
        assert_eq!(delimiter, '|');
    }

    #[test]
    fn test_parse_copy_not_from_stdin() {
        // COPY TO STDOUT should not match
        assert!(parse_copy_from_stdin("COPY mytable TO STDOUT").is_none());
        // Regular SELECT should not match
        assert!(parse_copy_from_stdin("SELECT * FROM mytable").is_none());
    }

    #[tokio::test]
    async fn test_copy_from_stdin_wire_protocol() {
        let port = start_test_server().await;
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
        send_startup(&mut stream).await;
        let _ = read_until_ready(&mut stream).await;

        // Create the table
        send_query(&mut stream, "CREATE TABLE copy_test (id INTEGER, name TEXT)").await;
        let _ = read_until_ready(&mut stream).await;

        // Send COPY FROM STDIN command
        send_query(&mut stream, "COPY copy_test FROM STDIN").await;

        // Read CopyInResponse (message type 'G')
        let mut header = [0u8; 5];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0], b'G', "Expected CopyInResponse");

        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut copy_in_body = vec![0u8; length - 4];
        if length > 4 {
            stream.read_exact(&mut copy_in_body).await.unwrap();
        }

        // Send CopyData messages (tab-separated)
        let row1 = b"1\tAlice\n";
        let row2 = b"2\tBob\n";

        // CopyData for row1
        let mut buf = BytesMut::new();
        buf.put_u8(b'd');
        buf.put_i32((4 + row1.len()) as i32);
        buf.extend_from_slice(row1);
        stream.write_all(&buf).await.unwrap();

        // CopyData for row2
        let mut buf = BytesMut::new();
        buf.put_u8(b'd');
        buf.put_i32((4 + row2.len()) as i32);
        buf.extend_from_slice(row2);
        stream.write_all(&buf).await.unwrap();

        // Send CopyDone
        let mut buf = BytesMut::new();
        buf.put_u8(b'c');
        buf.put_i32(4);
        stream.write_all(&buf).await.unwrap();

        // Read CommandComplete and ReadyForQuery
        let response = read_until_ready(&mut stream).await;
        // Should contain 'C' (CommandComplete) with "COPY 2"
        assert!(response.contains(&b'C'), "Expected CommandComplete");

        // Verify data was inserted
        send_query(&mut stream, "SELECT * FROM copy_test ORDER BY id").await;
        let response = read_until_ready(&mut stream).await;
        assert!(response.contains(&b'D'), "Expected DataRow");
    }
}
