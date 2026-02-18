/// Checks if a query is targeting pg_catalog or information_schema tables
/// and intercepts it with emulated results.
pub fn intercept_catalog_query(sql: &str) -> Option<CatalogQuery> {
    let lower = sql.to_lowercase();
    let trimmed = lower.trim();

    // Common pg_catalog queries from drivers/ORMs
    if trimmed.contains("pg_catalog.pg_type")
        || trimmed.contains("pg_type")
        || trimmed.starts_with("select typname")
    {
        return Some(CatalogQuery::PgType);
    }

    if trimmed.contains("pg_catalog.pg_namespace")
        || trimmed.contains("pg_namespace")
    {
        return Some(CatalogQuery::PgNamespace);
    }

    if trimmed.contains("information_schema.tables") {
        return Some(CatalogQuery::InformationSchemaTables);
    }

    if trimmed.contains("information_schema.columns") {
        return Some(CatalogQuery::InformationSchemaColumns);
    }

    if trimmed.contains("pg_catalog.pg_database") || trimmed.contains("pg_database") {
        return Some(CatalogQuery::PgDatabase);
    }

    if trimmed.contains("pg_catalog.pg_roles") || trimmed.contains("pg_roles")
        || trimmed.contains("pg_user")
    {
        return Some(CatalogQuery::PgRoles);
    }

    if trimmed.contains("pg_catalog.pg_settings") || trimmed.contains("pg_settings") {
        return Some(CatalogQuery::PgSettings);
    }

    if trimmed.contains("pg_catalog.pg_indexes") || trimmed.contains("pg_indexes") {
        return Some(CatalogQuery::PgIndexes);
    }

    if trimmed.contains("pg_catalog.pg_constraint") || trimmed.contains("pg_constraint") {
        return Some(CatalogQuery::PgConstraint);
    }

    if trimmed.contains("pg_catalog.pg_class") || trimmed.contains("pg_class") {
        return Some(CatalogQuery::PgClass);
    }

    if trimmed.contains("pg_catalog.pg_attribute") || trimmed.contains("pg_attribute") {
        return Some(CatalogQuery::PgAttribute);
    }

    // version() function
    if trimmed == "select version()" {
        return Some(CatalogQuery::Version);
    }

    // current_database()
    if trimmed.contains("current_database()") {
        return Some(CatalogQuery::CurrentDatabase);
    }

    // current_schema / current_schemas
    if trimmed.contains("current_schema") {
        return Some(CatalogQuery::CurrentSchema);
    }

    // current_user / session_user
    if trimmed.contains("current_user") || trimmed.contains("session_user") {
        return Some(CatalogQuery::CurrentUser);
    }

    None
}

#[derive(Debug, Clone)]
pub enum CatalogQuery {
    PgType,
    PgNamespace,
    PgClass,
    PgAttribute,
    PgDatabase,
    PgRoles,
    PgSettings,
    PgIndexes,
    PgConstraint,
    InformationSchemaTables,
    InformationSchemaColumns,
    Version,
    CurrentDatabase,
    CurrentSchema,
    CurrentUser,
}

/// Common PostgreSQL type OIDs
pub struct PgTypes;

impl PgTypes {
    /// Returns rows for a minimal pg_type catalog
    pub fn type_rows() -> Vec<Vec<Option<String>>> {
        vec![
            Self::type_row("bool", "16", "1", "b"),
            Self::type_row("bytea", "17", "-1", "b"),
            Self::type_row("int2", "21", "2", "b"),
            Self::type_row("int4", "23", "4", "b"),
            Self::type_row("int8", "20", "8", "b"),
            Self::type_row("float4", "700", "4", "b"),
            Self::type_row("float8", "701", "8", "b"),
            Self::type_row("text", "25", "-1", "b"),
            Self::type_row("varchar", "1043", "-1", "b"),
            Self::type_row("char", "18", "1", "b"),
            Self::type_row("bpchar", "1042", "-1", "b"),
            Self::type_row("name", "19", "64", "b"),
            Self::type_row("oid", "26", "4", "b"),
            Self::type_row("json", "114", "-1", "b"),
            Self::type_row("jsonb", "3802", "-1", "b"),
            Self::type_row("uuid", "2950", "16", "b"),
            Self::type_row("timestamp", "1114", "8", "b"),
            Self::type_row("timestamptz", "1184", "8", "b"),
            Self::type_row("date", "1082", "4", "b"),
            Self::type_row("time", "1083", "8", "b"),
            Self::type_row("interval", "1186", "16", "b"),
            Self::type_row("numeric", "1700", "-1", "b"),
            Self::type_row("void", "2278", "4", "p"),
            Self::type_row("_text", "1009", "-1", "b"),
            Self::type_row("_int4", "1007", "-1", "b"),
        ]
    }

    fn type_row(name: &str, oid: &str, len: &str, typtype: &str) -> Vec<Option<String>> {
        vec![
            Some(oid.to_string()),         // oid
            Some(name.to_string()),        // typname
            Some("11".to_string()),        // typnamespace (pg_catalog)
            Some(len.to_string()),         // typlen
            Some(typtype.to_string()),     // typtype
        ]
    }

    pub fn type_columns() -> Vec<String> {
        vec![
            "oid".to_string(),
            "typname".to_string(),
            "typnamespace".to_string(),
            "typlen".to_string(),
            "typtype".to_string(),
        ]
    }
}

pub fn get_catalog_result(query: CatalogQuery) -> CatalogResult {
    match query {
        CatalogQuery::PgType => CatalogResult {
            columns: PgTypes::type_columns(),
            rows: PgTypes::type_rows(),
            tag: format!("SELECT {}", PgTypes::type_rows().len()),
        },
        CatalogQuery::PgNamespace => CatalogResult {
            columns: vec!["oid".into(), "nspname".into(), "nspowner".into()],
            rows: vec![
                vec![Some("11".into()), Some("pg_catalog".into()), Some("10".into())],
                vec![Some("2200".into()), Some("public".into()), Some("10".into())],
                vec![Some("11713".into()), Some("information_schema".into()), Some("10".into())],
            ],
            tag: "SELECT 3".to_string(),
        },
        CatalogQuery::PgDatabase => CatalogResult {
            columns: vec![
                "oid".into(), "datname".into(), "datdba".into(),
                "encoding".into(), "datcollate".into(), "datctype".into(),
            ],
            rows: vec![vec![
                Some("16384".into()),           // oid
                Some("pgsqlite".into()),        // datname
                Some("10".into()),              // datdba (owner OID)
                Some("6".into()),               // encoding (UTF8 = 6)
                Some("en_US.UTF-8".into()),     // datcollate
                Some("en_US.UTF-8".into()),     // datctype
            ]],
            tag: "SELECT 1".to_string(),
        },
        CatalogQuery::PgRoles => CatalogResult {
            columns: vec![
                "oid".into(), "rolname".into(), "rolsuper".into(),
                "rolinherit".into(), "rolcreaterole".into(), "rolcreatedb".into(),
                "rolcanlogin".into(), "rolreplication".into(),
            ],
            rows: vec![vec![
                Some("10".into()),              // oid
                Some("pgsqlite".into()),        // rolname
                Some("t".into()),               // rolsuper
                Some("t".into()),               // rolinherit
                Some("t".into()),               // rolcreaterole
                Some("t".into()),               // rolcreatedb
                Some("t".into()),               // rolcanlogin
                Some("f".into()),               // rolreplication
            ]],
            tag: "SELECT 1".to_string(),
        },
        CatalogQuery::PgSettings => CatalogResult {
            columns: vec![
                "name".into(), "setting".into(), "unit".into(),
                "category".into(), "short_desc".into(),
            ],
            rows: vec![
                vec![Some("server_version".into()), Some("15.0.0".into()), None, Some("Preset Options".into()), Some("Server version".into())],
                vec![Some("server_encoding".into()), Some("UTF8".into()), None, Some("Client Connection Defaults".into()), Some("Server encoding".into())],
                vec![Some("client_encoding".into()), Some("UTF8".into()), None, Some("Client Connection Defaults".into()), Some("Client encoding".into())],
                vec![Some("lc_collate".into()), Some("en_US.UTF-8".into()), None, Some("Preset Options".into()), Some("Locale collation".into())],
                vec![Some("lc_ctype".into()), Some("en_US.UTF-8".into()), None, Some("Preset Options".into()), Some("Locale character classification".into())],
                vec![Some("TimeZone".into()), Some("UTC".into()), None, Some("Client Connection Defaults".into()), Some("Time zone".into())],
                vec![Some("DateStyle".into()), Some("ISO, MDY".into()), None, Some("Client Connection Defaults".into()), Some("Date display format".into())],
                vec![Some("max_connections".into()), Some("100".into()), None, Some("Connections and Authentication".into()), Some("Maximum concurrent connections".into())],
                vec![Some("standard_conforming_strings".into()), Some("on".into()), None, Some("Client Connection Defaults".into()), Some("Standard conforming strings".into())],
                vec![Some("integer_datetimes".into()), Some("on".into()), None, Some("Preset Options".into()), Some("Integer datetimes".into())],
            ],
            tag: "SELECT 10".to_string(),
        },
        CatalogQuery::PgIndexes => CatalogResult {
            columns: vec![
                "schemaname".into(), "tablename".into(),
                "indexname".into(), "indexdef".into(),
            ],
            rows: vec![],
            tag: "SELECT 0".to_string(),
        },
        CatalogQuery::PgConstraint => CatalogResult {
            columns: vec![
                "oid".into(), "conname".into(), "connamespace".into(),
                "contype".into(), "conrelid".into(), "confrelid".into(),
            ],
            rows: vec![],
            tag: "SELECT 0".to_string(),
        },
        CatalogQuery::PgClass => CatalogResult {
            columns: vec!["oid".into(), "relname".into(), "relnamespace".into(), "relkind".into()],
            rows: vec![],
            tag: "SELECT 0".to_string(),
        },
        CatalogQuery::PgAttribute => CatalogResult {
            columns: vec!["attrelid".into(), "attname".into(), "atttypid".into(), "attnum".into()],
            rows: vec![],
            tag: "SELECT 0".to_string(),
        },
        CatalogQuery::InformationSchemaTables => CatalogResult {
            columns: vec![
                "table_catalog".into(), "table_schema".into(),
                "table_name".into(), "table_type".into(),
            ],
            rows: vec![],
            tag: "SELECT 0".to_string(),
        },
        CatalogQuery::InformationSchemaColumns => CatalogResult {
            columns: vec![
                "table_catalog".into(), "table_schema".into(),
                "table_name".into(), "column_name".into(),
                "data_type".into(), "is_nullable".into(),
            ],
            rows: vec![],
            tag: "SELECT 0".to_string(),
        },
        CatalogQuery::Version => CatalogResult {
            columns: vec!["version".into()],
            rows: vec![vec![Some("PostgreSQL 15.0.0 (pgsqlite)".into())]],
            tag: "SELECT 1".to_string(),
        },
        CatalogQuery::CurrentDatabase => CatalogResult {
            columns: vec!["current_database".into()],
            rows: vec![vec![Some("pgsqlite".into())]],
            tag: "SELECT 1".to_string(),
        },
        CatalogQuery::CurrentSchema => CatalogResult {
            columns: vec!["current_schema".into()],
            rows: vec![vec![Some("public".into())]],
            tag: "SELECT 1".to_string(),
        },
        CatalogQuery::CurrentUser => CatalogResult {
            columns: vec!["current_user".into()],
            rows: vec![vec![Some("pgsqlite".into())]],
            tag: "SELECT 1".to_string(),
        },
    }
}

pub struct CatalogResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub tag: String,
}

use crate::backend::sqlite::SqliteBackend;

/// Get catalog result with dynamic data from SQLite metadata
pub fn get_dynamic_catalog_result(query: CatalogQuery, backend: &SqliteBackend) -> CatalogResult {
    match query {
        CatalogQuery::InformationSchemaTables => get_information_schema_tables(backend),
        CatalogQuery::InformationSchemaColumns => get_information_schema_columns(backend),
        CatalogQuery::PgClass => get_pg_class(backend),
        CatalogQuery::PgAttribute => get_pg_attribute(backend),
        CatalogQuery::PgIndexes => get_pg_indexes(backend),
        CatalogQuery::PgConstraint => get_pg_constraint(backend),
        // Delegate to static results for queries that don't need dynamic data
        other => get_catalog_result(other),
    }
}

fn get_information_schema_tables(backend: &SqliteBackend) -> CatalogResult {
    let tables = backend.query_sqlite_master_tables();
    let rows: Vec<Vec<Option<String>>> = tables.iter().map(|name| {
        vec![
            Some("pgsqlite".into()),   // table_catalog
            Some("public".into()),     // table_schema
            Some(name.clone()),        // table_name
            Some("BASE TABLE".into()), // table_type
        ]
    }).collect();
    let count = rows.len();
    CatalogResult {
        columns: vec![
            "table_catalog".into(), "table_schema".into(),
            "table_name".into(), "table_type".into(),
        ],
        rows,
        tag: format!("SELECT {}", count),
    }
}

fn get_information_schema_columns(backend: &SqliteBackend) -> CatalogResult {
    let column_info = backend.query_sqlite_master_columns();
    let rows: Vec<Vec<Option<String>>> = column_info.iter().map(|(table, col_name, col_type, nullable, ordinal)| {
        vec![
            Some("pgsqlite".into()),     // table_catalog
            Some("public".into()),       // table_schema
            Some(table.clone()),         // table_name
            Some(col_name.clone()),      // column_name
            Some(ordinal.to_string()),   // ordinal_position
            Some(col_type.clone()),      // data_type
            Some(if *nullable { "YES" } else { "NO" }.into()), // is_nullable
        ]
    }).collect();
    let count = rows.len();
    CatalogResult {
        columns: vec![
            "table_catalog".into(), "table_schema".into(),
            "table_name".into(), "column_name".into(),
            "ordinal_position".into(), "data_type".into(),
            "is_nullable".into(),
        ],
        rows,
        tag: format!("SELECT {}", count),
    }
}

fn get_pg_class(backend: &SqliteBackend) -> CatalogResult {
    let tables = backend.query_sqlite_master_tables();
    let rows: Vec<Vec<Option<String>>> = tables.iter().enumerate().map(|(i, name)| {
        vec![
            Some(format!("{}", 16384 + i)),  // oid (synthetic)
            Some(name.clone()),               // relname
            Some("2200".into()),              // relnamespace (public)
            Some("r".into()),                 // relkind (regular table)
        ]
    }).collect();
    let count = rows.len();
    CatalogResult {
        columns: vec!["oid".into(), "relname".into(), "relnamespace".into(), "relkind".into()],
        rows,
        tag: format!("SELECT {}", count),
    }
}

fn get_pg_attribute(backend: &SqliteBackend) -> CatalogResult {
    let column_info = backend.query_sqlite_master_columns();
    let tables = backend.query_sqlite_master_tables();
    let rows: Vec<Vec<Option<String>>> = column_info.iter().map(|(table, col_name, col_type, _, _)| {
        let table_oid = tables.iter().position(|t| t == table)
            .map(|i| format!("{}", 16384 + i))
            .unwrap_or_else(|| "0".into());
        let type_oid = sqlite_type_to_pg_oid(col_type);
        vec![
            Some(table_oid),                 // attrelid
            Some(col_name.clone()),          // attname
            Some(type_oid.to_string()),      // atttypid
            Some("0".into()),               // attnum
        ]
    }).collect();
    let count = rows.len();
    CatalogResult {
        columns: vec!["attrelid".into(), "attname".into(), "atttypid".into(), "attnum".into()],
        rows,
        tag: format!("SELECT {}", count),
    }
}

fn get_pg_indexes(backend: &SqliteBackend) -> CatalogResult {
    let indexes = backend.query_sqlite_master_indexes();
    let rows: Vec<Vec<Option<String>>> = indexes.iter().map(|(name, table, sql)| {
        vec![
            Some("public".into()),         // schemaname
            Some(table.clone()),            // tablename
            Some(name.clone()),             // indexname
            sql.clone().or_else(|| Some(String::new())),  // indexdef
        ]
    }).collect();
    let count = rows.len();
    CatalogResult {
        columns: vec![
            "schemaname".into(), "tablename".into(),
            "indexname".into(), "indexdef".into(),
        ],
        rows,
        tag: format!("SELECT {}", count),
    }
}

fn get_pg_constraint(backend: &SqliteBackend) -> CatalogResult {
    let constraints = backend.query_constraints();
    let tables = backend.query_sqlite_master_tables();
    let rows: Vec<Vec<Option<String>>> = constraints.iter().enumerate().map(|(i, (table, col, con_type))| {
        let table_oid = tables.iter().position(|t| t == table)
            .map(|idx| format!("{}", 16384 + idx))
            .unwrap_or_else(|| "0".into());
        let contype = match con_type.as_str() {
            "PRIMARY KEY" => "p",
            "NOT NULL" => "n",
            "UNIQUE" => "u",
            "FOREIGN KEY" => "f",
            _ => "c", // check
        };
        let conname = format!("{}_{}_{}",
            table,
            col,
            match contype { "p" => "pkey", "u" => "key", "n" => "nn", "f" => "fkey", _ => "check" }
        );
        vec![
            Some(format!("{}", 32768 + i)),  // oid (synthetic)
            Some(conname),                    // conname
            Some("2200".into()),             // connamespace (public)
            Some(contype.into()),            // contype
            Some(table_oid),                 // conrelid
            Some("0".into()),               // confrelid
        ]
    }).collect();
    let count = rows.len();
    CatalogResult {
        columns: vec![
            "oid".into(), "conname".into(), "connamespace".into(),
            "contype".into(), "conrelid".into(), "confrelid".into(),
        ],
        rows,
        tag: format!("SELECT {}", count),
    }
}

fn sqlite_type_to_pg_oid(sqlite_type: &str) -> i32 {
    let upper = sqlite_type.to_uppercase();
    let base = if let Some(idx) = upper.find('(') { &upper[..idx] } else { &upper };
    match base.trim() {
        "INTEGER" | "INT" | "INT4" => 23,
        "BIGINT" | "INT8" => 20,
        "SMALLINT" | "INT2" => 21,
        "REAL" | "FLOAT" | "FLOAT4" => 700,
        "DOUBLE PRECISION" | "FLOAT8" => 701,
        "TEXT" | "CLOB" => 25,
        "VARCHAR" | "CHARACTER VARYING" => 1043,
        "BLOB" => 17,
        "BOOLEAN" | "BOOL" => 16,
        "NUMERIC" | "DECIMAL" => 1700,
        _ => 25,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intercept_pg_type_query() {
        let result = intercept_catalog_query("SELECT * FROM pg_catalog.pg_type");
        assert!(matches!(result, Some(CatalogQuery::PgType)));
    }

    #[test]
    fn test_intercept_pg_type_without_schema() {
        let result = intercept_catalog_query("SELECT * FROM pg_type WHERE typname = 'int4'");
        assert!(matches!(result, Some(CatalogQuery::PgType)));
    }

    #[test]
    fn test_intercept_pg_namespace() {
        let result = intercept_catalog_query("SELECT * FROM pg_catalog.pg_namespace");
        assert!(matches!(result, Some(CatalogQuery::PgNamespace)));
    }

    #[test]
    fn test_intercept_information_schema_tables() {
        let result = intercept_catalog_query(
            "SELECT * FROM information_schema.tables WHERE table_schema = 'public'"
        );
        assert!(matches!(result, Some(CatalogQuery::InformationSchemaTables)));
    }

    #[test]
    fn test_intercept_version() {
        let result = intercept_catalog_query("SELECT version()");
        assert!(matches!(result, Some(CatalogQuery::Version)));
    }

    #[test]
    fn test_intercept_current_database() {
        let result = intercept_catalog_query("SELECT current_database()");
        assert!(matches!(result, Some(CatalogQuery::CurrentDatabase)));
    }

    #[test]
    fn test_no_intercept_normal_query() {
        let result = intercept_catalog_query("SELECT * FROM users");
        assert!(result.is_none());
    }

    #[test]
    fn test_pg_type_rows_structure() {
        let rows = PgTypes::type_rows();
        assert!(rows.len() > 20, "Should have common PG types");
        // Check int4 is present
        let int4 = rows.iter().find(|r| r[1] == Some("int4".into()));
        assert!(int4.is_some());
        let int4 = int4.unwrap();
        assert_eq!(int4[0], Some("23".into())); // OID
    }

    #[test]
    fn test_version_result() {
        let result = get_catalog_result(CatalogQuery::Version);
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0][0].as_ref().unwrap().contains("pgsqlite"));
    }

    #[test]
    fn test_namespace_result() {
        let result = get_catalog_result(CatalogQuery::PgNamespace);
        assert_eq!(result.rows.len(), 3);
        let names: Vec<&str> = result.rows.iter()
            .map(|r| r[1].as_ref().unwrap().as_str())
            .collect();
        assert!(names.contains(&"public"));
        assert!(names.contains(&"pg_catalog"));
    }

    #[test]
    fn test_current_schema() {
        let result = intercept_catalog_query("SELECT current_schema()");
        assert!(matches!(result, Some(CatalogQuery::CurrentSchema)));
        let result = get_catalog_result(CatalogQuery::CurrentSchema);
        assert_eq!(result.rows[0][0], Some("public".into()));
    }

    #[test]
    fn test_dynamic_information_schema_tables() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        backend.execute("CREATE TABLE posts (id INTEGER, title TEXT)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::InformationSchemaTables, &backend);
        assert_eq!(result.rows.len(), 2, "Should have 2 tables");
        let names: Vec<&str> = result.rows.iter()
            .map(|r| r[2].as_ref().unwrap().as_str())
            .collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"posts"));
        // Check schema
        assert_eq!(result.rows[0][1], Some("public".into()));
        assert_eq!(result.rows[0][3], Some("BASE TABLE".into()));
    }

    #[test]
    fn test_dynamic_information_schema_columns() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT, email TEXT NOT NULL)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::InformationSchemaColumns, &backend);
        assert_eq!(result.rows.len(), 3, "Should have 3 columns");

        // Find id column (column_name is at index 3)
        let id_row = result.rows.iter().find(|r| r[3] == Some("id".into())).unwrap();
        assert_eq!(id_row[2], Some("users".into()));    // table_name
        assert_eq!(id_row[4], Some("1".into()));         // ordinal_position
        assert_eq!(id_row[5], Some("INTEGER".into()));   // data_type
        assert_eq!(id_row[6], Some("NO".into()));        // is_nullable (NOT NULL)

        // Find name column
        let name_row = result.rows.iter().find(|r| r[3] == Some("name".into())).unwrap();
        assert_eq!(name_row[5], Some("TEXT".into()));    // data_type
        assert_eq!(name_row[6], Some("YES".into()));     // is_nullable
    }

    #[test]
    fn test_dynamic_pg_class() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE items (id INTEGER)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::PgClass, &backend);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("items".into()));
        assert_eq!(result.rows[0][3], Some("r".into()));
    }

    #[test]
    fn test_dynamic_pg_attribute() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::PgAttribute, &backend);
        assert_eq!(result.rows.len(), 2);

        let names: Vec<&str> = result.rows.iter()
            .map(|r| r[1].as_ref().unwrap().as_str())
            .collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
    }

    #[test]
    fn test_dynamic_empty_database() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        let result = get_dynamic_catalog_result(CatalogQuery::InformationSchemaTables, &backend);
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_intercept_pg_database() {
        let result = intercept_catalog_query("SELECT * FROM pg_database");
        assert!(matches!(result, Some(CatalogQuery::PgDatabase)));
        let result = intercept_catalog_query("SELECT * FROM pg_catalog.pg_database");
        assert!(matches!(result, Some(CatalogQuery::PgDatabase)));
    }

    #[test]
    fn test_pg_database_result() {
        let result = get_catalog_result(CatalogQuery::PgDatabase);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("pgsqlite".into())); // datname
    }

    #[test]
    fn test_intercept_pg_roles() {
        let result = intercept_catalog_query("SELECT * FROM pg_roles");
        assert!(matches!(result, Some(CatalogQuery::PgRoles)));
        let result = intercept_catalog_query("SELECT * FROM pg_user");
        assert!(matches!(result, Some(CatalogQuery::PgRoles)));
    }

    #[test]
    fn test_pg_roles_result() {
        let result = get_catalog_result(CatalogQuery::PgRoles);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Some("pgsqlite".into())); // rolname
        assert_eq!(result.rows[0][2], Some("t".into()));        // rolsuper
    }

    #[test]
    fn test_intercept_pg_settings() {
        let result = intercept_catalog_query("SELECT * FROM pg_settings");
        assert!(matches!(result, Some(CatalogQuery::PgSettings)));
        let result = intercept_catalog_query("SELECT * FROM pg_catalog.pg_settings");
        assert!(matches!(result, Some(CatalogQuery::PgSettings)));
    }

    #[test]
    fn test_pg_settings_result() {
        let result = get_catalog_result(CatalogQuery::PgSettings);
        assert!(result.rows.len() >= 10, "Should have at least 10 settings");
        let names: Vec<&str> = result.rows.iter()
            .map(|r| r[0].as_ref().unwrap().as_str())
            .collect();
        assert!(names.contains(&"server_version"));
        assert!(names.contains(&"TimeZone"));
        assert!(names.contains(&"max_connections"));
    }

    #[test]
    fn test_intercept_pg_indexes() {
        let result = intercept_catalog_query("SELECT * FROM pg_indexes");
        assert!(matches!(result, Some(CatalogQuery::PgIndexes)));
    }

    #[test]
    fn test_dynamic_pg_indexes() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE idx_t (id INTEGER, name TEXT)").unwrap();
        backend.execute("CREATE INDEX idx_name ON idx_t (name)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::PgIndexes, &backend);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Some("public".into()));    // schemaname
        assert_eq!(result.rows[0][1], Some("idx_t".into()));     // tablename
        assert_eq!(result.rows[0][2], Some("idx_name".into()));  // indexname
    }

    #[test]
    fn test_intercept_pg_constraint() {
        let result = intercept_catalog_query("SELECT * FROM pg_constraint");
        assert!(matches!(result, Some(CatalogQuery::PgConstraint)));
    }

    #[test]
    fn test_dynamic_pg_constraint() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE con_t (id INTEGER PRIMARY KEY NOT NULL, name TEXT)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::PgConstraint, &backend);
        assert!(!result.rows.is_empty(), "Should have constraints");
        let types: Vec<&str> = result.rows.iter()
            .map(|r| r[3].as_ref().unwrap().as_str())
            .collect();
        assert!(types.contains(&"p"), "Should have PRIMARY KEY constraint, got: {:?}", types);
    }

    #[test]
    fn test_dynamic_pg_indexes_empty() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE no_idx (id INTEGER)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::PgIndexes, &backend);
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_dynamic_pg_constraint_multiple() {
        let backend = SqliteBackend::new(":memory:").unwrap();
        backend.execute("CREATE TABLE multi_con (id INTEGER PRIMARY KEY NOT NULL, email TEXT NOT NULL, name TEXT)").unwrap();

        let result = get_dynamic_catalog_result(CatalogQuery::PgConstraint, &backend);
        // Should have at least: id PRIMARY KEY, id NOT NULL, email NOT NULL
        assert!(result.rows.len() >= 3, "Should have at least 3 constraints, got {}", result.rows.len());
    }
}
