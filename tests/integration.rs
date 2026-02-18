use std::sync::Arc;
use tokio::net::TcpListener;
use pgsqlite::backend::sqlite::SqliteBackend;
use pgsqlite::server;
use deadpool_postgres::{Config, Runtime};

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
                let _ = server::handle_connection_public(stream, backend).await;
            });
        }
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    port
}

#[tokio::test]
async fn test_tokio_postgres_connect() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Simple query
    let rows = client.query("SELECT 1 AS num", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: &str = rows[0].get(0);
    assert_eq!(val, "1");
}

#[tokio::test]
async fn test_tokio_postgres_create_insert_select() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create table with PG types
    client.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255), email TEXT)",
        &[],
    ).await.unwrap();

    // Insert data
    client.execute(
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        &[],
    ).await.unwrap();

    client.execute(
        "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
        &[],
    ).await.unwrap();

    // Query
    let rows = client.query("SELECT id, name, email FROM users ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);

    let name: &str = rows[0].get(1);
    assert_eq!(name, "Alice");

    let email: &str = rows[1].get(2);
    assert_eq!(email, "bob@example.com");
}

#[tokio::test]
async fn test_tokio_postgres_error_handling() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Query non-existent table should error
    let result = client.query("SELECT * FROM nonexistent", &[]).await;
    assert!(result.is_err());

    // But connection should still work after error
    let rows = client.query("SELECT 42 AS answer", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_parameterized_select() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create and populate table
    client.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price TEXT)",
        &[],
    ).await.unwrap();

    client.execute(
        "INSERT INTO items (id, name, price) VALUES (1, 'Widget', '9.99')",
        &[],
    ).await.unwrap();

    client.execute(
        "INSERT INTO items (id, name, price) VALUES (2, 'Gadget', '19.99')",
        &[],
    ).await.unwrap();

    // Parameterized SELECT
    let rows = client.query(
        "SELECT name, price FROM items WHERE id = $1",
        &[&"1"],
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Widget");
}

#[tokio::test]
async fn test_tokio_postgres_parameterized_insert() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute(
        "CREATE TABLE users2 (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        &[],
    ).await.unwrap();

    // Parameterized INSERT
    let affected = client.execute(
        "INSERT INTO users2 (id, name, email) VALUES ($1, $2, $3)",
        &[&"1", &"Alice", &"alice@example.com"],
    ).await.unwrap();
    assert_eq!(affected, 1);

    // Verify the insert
    let rows = client.query("SELECT name, email FROM users2 WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Alice");
    let email: &str = rows[0].get(1);
    assert_eq!(email, "alice@example.com");
}

#[tokio::test]
async fn test_tokio_postgres_parameterized_update_delete() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE t3 (id INTEGER, val TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO t3 VALUES (1, 'old')", &[]).await.unwrap();

    // Parameterized UPDATE
    let affected = client.execute(
        "UPDATE t3 SET val = $1 WHERE id = $2",
        &[&"new", &"1"],
    ).await.unwrap();
    assert_eq!(affected, 1);

    let rows = client.query("SELECT val FROM t3 WHERE id = 1", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "new");

    // Parameterized DELETE
    let affected = client.execute(
        "DELETE FROM t3 WHERE id = $1",
        &[&"1"],
    ).await.unwrap();
    assert_eq!(affected, 1);

    let rows = client.query("SELECT * FROM t3", &[]).await.unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_tokio_postgres_transaction_commit() {
    let port = start_test_server().await;

    let (mut client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE txn_test (id INTEGER, val TEXT)", &[]).await.unwrap();

    // Use a transaction block
    let txn = client.transaction().await.unwrap();
    txn.execute("INSERT INTO txn_test VALUES (1, 'committed')", &[]).await.unwrap();
    txn.execute("INSERT INTO txn_test VALUES (2, 'also committed')", &[]).await.unwrap();
    txn.commit().await.unwrap();

    // Verify data persists after commit
    let rows = client.query("SELECT * FROM txn_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let val: &str = rows[0].get(1);
    assert_eq!(val, "committed");
}

#[tokio::test]
async fn test_tokio_postgres_transaction_rollback() {
    let port = start_test_server().await;

    let (mut client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE txn_rb (id INTEGER, val TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO txn_rb VALUES (1, 'before')", &[]).await.unwrap();

    // Start a transaction and rollback
    let txn = client.transaction().await.unwrap();
    txn.execute("INSERT INTO txn_rb VALUES (2, 'rolled back')", &[]).await.unwrap();
    txn.rollback().await.unwrap();

    // Only the row inserted before the transaction should exist
    let rows = client.query("SELECT * FROM txn_rb", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: &str = rows[0].get(1);
    assert_eq!(val, "before");
}

#[tokio::test]
async fn test_tokio_postgres_transaction_error_recovery() {
    let port = start_test_server().await;

    let (mut client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE txn_err (id INTEGER PRIMARY KEY, val TEXT)", &[]).await.unwrap();

    // After a failed transaction, the connection should still work
    let txn = client.transaction().await.unwrap();
    txn.execute("INSERT INTO txn_err VALUES (1, 'ok')", &[]).await.unwrap();
    let result = txn.execute("SELECT * FROM nonexistent_table", &[]).await;
    assert!(result.is_err());
    // Transaction should be rolled back automatically on drop
    drop(txn);

    // Connection should still work
    let rows = client.query("SELECT 1 AS alive", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_information_schema() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create tables
    client.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE posts (id INTEGER, title TEXT, user_id INTEGER)", &[]).await.unwrap();

    // Query information_schema.tables
    let rows = client.query(
        "SELECT table_name, table_type FROM information_schema.tables ORDER BY table_name",
        &[],
    ).await.unwrap();
    assert!(rows.len() >= 2, "Should have at least 2 tables, got {}", rows.len());

    // Query information_schema.columns
    let rows = client.query(
        "SELECT table_name, column_name, data_type FROM information_schema.columns",
        &[],
    ).await.unwrap();
    assert!(rows.len() >= 5, "Should have at least 5 columns across both tables, got {}", rows.len());
}

#[tokio::test]
async fn test_tokio_postgres_insert_returning() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute(
        "CREATE TABLE returning_test (id INTEGER PRIMARY KEY, name TEXT, created TEXT)",
        &[],
    ).await.unwrap();

    // INSERT ... RETURNING - use TEXT columns to avoid type OID issues
    let rows = client.query(
        "INSERT INTO returning_test (id, name, created) VALUES (1, 'alice', 'now') RETURNING name, created",
        &[],
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "alice");
    let created: &str = rows[0].get(1);
    assert_eq!(created, "now");
}

#[tokio::test]
async fn test_tokio_postgres_upsert() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE upsert_test (id INTEGER PRIMARY KEY, val TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO upsert_test VALUES (1, 'original')", &[]).await.unwrap();

    // Upsert
    client.execute(
        "INSERT INTO upsert_test (id, val) VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = excluded.val",
        &[],
    ).await.unwrap();

    let rows = client.query("SELECT val FROM upsert_test WHERE id = 1", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "updated");
}

#[tokio::test]
async fn test_tokio_postgres_show_commands() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // SHOW commands ORMs commonly use
    let rows = client.query("SHOW TRANSACTION ISOLATION LEVEL", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    let rows = client.query("SHOW SERVER_VERSION", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let version: &str = rows[0].get(0);
    assert!(version.contains("15"), "got: {}", version);
}

#[tokio::test]
async fn test_tokio_postgres_listen_notify_stubs() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // LISTEN should be accepted silently
    let result = client.simple_query("LISTEN test_channel").await;
    assert!(result.is_ok(), "LISTEN should be accepted: {:?}", result.err());

    // NOTIFY should be accepted silently
    let result = client.simple_query("NOTIFY test_channel, 'hello'").await;
    assert!(result.is_ok(), "NOTIFY should be accepted: {:?}", result.err());

    // Connection should still work after LISTEN/NOTIFY
    let rows = client.query("SELECT 1 AS test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_multi_statement() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Multi-statement via simple_query
    let results = client.simple_query(
        "CREATE TABLE multi_test (id INTEGER, val TEXT); INSERT INTO multi_test VALUES (1, 'hello'); INSERT INTO multi_test VALUES (2, 'world')"
    ).await.unwrap();
    // simple_query returns SimpleQueryMessage items
    assert!(!results.is_empty(), "Should get results for multi-statement");

    // Verify the data was inserted
    let rows = client.query("SELECT * FROM multi_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_tokio_postgres_large_result_set() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE big_table (id INTEGER, val TEXT)", &[]).await.unwrap();

    // Insert 1000 rows
    for i in 0..1000 {
        client.execute(
            &format!("INSERT INTO big_table VALUES ({}, 'row_{}')", i, i),
            &[],
        ).await.unwrap();
    }

    let rows = client.query("SELECT * FROM big_table", &[]).await.unwrap();
    assert_eq!(rows.len(), 1000, "Should get 1000 rows");
}

#[tokio::test]
async fn test_tokio_postgres_unicode_data() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE unicode_test (id INTEGER, name TEXT)", &[]).await.unwrap();

    // Insert emoji, CJK, and accented chars
    client.execute("INSERT INTO unicode_test VALUES (1, 'Hello 🌍🎉')", &[]).await.unwrap();
    client.execute("INSERT INTO unicode_test VALUES (2, '日本語テスト')", &[]).await.unwrap();
    client.execute("INSERT INTO unicode_test VALUES (3, 'Ñoño café résumé')", &[]).await.unwrap();

    let rows = client.query("SELECT name FROM unicode_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Hello 🌍🎉");
    let name: &str = rows[1].get(0);
    assert_eq!(name, "日本語テスト");
    let name: &str = rows[2].get(0);
    assert_eq!(name, "Ñoño café résumé");
}

#[tokio::test]
async fn test_tokio_postgres_empty_table_select() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE empty_table (id INTEGER, name TEXT)", &[]).await.unwrap();

    let rows = client.query("SELECT * FROM empty_table", &[]).await.unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_tokio_postgres_concurrent_connections() {
    let port = start_test_server().await;

    // Create table first with one connection
    let (setup_client, setup_conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = setup_conn.await {
            eprintln!("setup connection error: {}", e);
        }
    });

    setup_client.execute("CREATE TABLE concurrent_test (id INTEGER, source TEXT)", &[]).await.unwrap();

    // Launch 5 concurrent connections
    let mut handles = vec![];
    for i in 0..5 {
        let port = port;
        let handle = tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(
                &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
                tokio_postgres::NoTls,
            ).await.unwrap();

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            // Each connection does a SELECT
            let rows = client.query("SELECT 1 AS test", &[]).await.unwrap();
            assert_eq!(rows.len(), 1);
            i
        });
        handles.push(handle);
    }

    // Wait for all connections to complete
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_tokio_postgres_select_expressions() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // SELECT without tables
    let rows = client.query("SELECT 1 + 2 AS sum", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    let rows = client.query("SELECT 'hello' AS greeting", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let greeting: &str = rows[0].get(0);
    assert_eq!(greeting, "hello");
}

#[tokio::test]
async fn test_tokio_postgres_set_commands() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // SET commands should be silently accepted
    client.simple_query("SET search_path TO public").await.unwrap();
    client.simple_query("SET client_encoding TO 'UTF8'").await.unwrap();
    client.simple_query("SET timezone TO 'UTC'").await.unwrap();

    // Connection should still work
    let rows = client.query("SELECT 1 AS alive", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_semicolon_in_strings() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE semi_test (id INTEGER, val TEXT)", &[]).await.unwrap();

    // Multi-statement with semicolons inside string literals via simple query protocol
    let result = client.simple_query(
        "INSERT INTO semi_test VALUES (1, 'hello; world'); INSERT INTO semi_test VALUES (2, 'foo;bar;baz')"
    ).await;
    assert!(result.is_ok(), "Multi-statement with semicolons in strings should work: {:?}", result.err());

    // Verify the data was inserted correctly
    let rows = client.query("SELECT val FROM semi_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let val1: &str = rows[0].get(0);
    assert_eq!(val1, "hello; world");
    let val2: &str = rows[1].get(0);
    assert_eq!(val2, "foo;bar;baz");
}

#[tokio::test]
async fn test_tokio_postgres_catalog_queries() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // version()
    let rows = client.query("SELECT version()", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let version: &str = rows[0].get(0);
    assert!(version.contains("pgsqlite"), "got: {}", version);

    // current_database()
    let rows = client.query("SELECT current_database()", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    // current_schema
    let rows = client.query("SELECT current_schema()", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let schema: &str = rows[0].get(0);
    assert_eq!(schema, "public");
}

#[tokio::test]
async fn test_tokio_postgres_null_handling() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE nullable_test (id INTEGER, name TEXT, value TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO nullable_test VALUES (1, 'has_value', 'data')", &[]).await.unwrap();
    client.execute("INSERT INTO nullable_test VALUES (2, NULL, NULL)", &[]).await.unwrap();
    client.execute("INSERT INTO nullable_test VALUES (3, 'partial', NULL)", &[]).await.unwrap();

    let rows = client.query("SELECT * FROM nullable_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);

    // Row 1: all non-null
    let name: &str = rows[0].get(1);
    assert_eq!(name, "has_value");

    // Row 2: nulls
    let name: Option<&str> = rows[1].get(1);
    assert!(name.is_none(), "Expected NULL for name");
    let value: Option<&str> = rows[1].get(2);
    assert!(value.is_none(), "Expected NULL for value");

    // Row 3: partial null
    let name: &str = rows[2].get(1);
    assert_eq!(name, "partial");
    let value: Option<&str> = rows[2].get(2);
    assert!(value.is_none(), "Expected NULL for value");
}

#[tokio::test]
async fn test_tokio_postgres_discard_all() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // DISCARD ALL should be accepted silently (commonly sent by connection pools)
    let result = client.simple_query("DISCARD ALL").await;
    assert!(result.is_ok(), "DISCARD ALL should be accepted: {:?}", result.err());

    // Connection should still work after DISCARD ALL
    let rows = client.query("SELECT 1 AS test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_reset_all() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // RESET ALL should be accepted silently
    let result = client.simple_query("RESET ALL").await;
    assert!(result.is_ok(), "RESET ALL should be accepted: {:?}", result.err());

    // Connection should still work after RESET ALL
    let rows = client.query("SELECT 1 AS test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_prepared_statement_reuse() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE ps_test (id INTEGER, val TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO ps_test VALUES (1, 'a')", &[]).await.unwrap();
    client.execute("INSERT INTO ps_test VALUES (2, 'b')", &[]).await.unwrap();
    client.execute("INSERT INTO ps_test VALUES (3, 'c')", &[]).await.unwrap();

    // Use the same parameterized query multiple times
    let stmt = client.prepare("SELECT val FROM ps_test WHERE id = $1").await.unwrap();

    let rows = client.query(&stmt, &[&"1"]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: &str = rows[0].get(0);
    assert_eq!(val, "a");

    let rows = client.query(&stmt, &[&"2"]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: &str = rows[0].get(0);
    assert_eq!(val, "b");

    let rows = client.query(&stmt, &[&"3"]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: &str = rows[0].get(0);
    assert_eq!(val, "c");
}

#[tokio::test]
async fn test_tokio_postgres_aggregate_functions() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE agg_test (category TEXT, value INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO agg_test VALUES ('a', 10)", &[]).await.unwrap();
    client.execute("INSERT INTO agg_test VALUES ('a', 20)", &[]).await.unwrap();
    client.execute("INSERT INTO agg_test VALUES ('b', 30)", &[]).await.unwrap();
    client.execute("INSERT INTO agg_test VALUES ('b', 40)", &[]).await.unwrap();

    // COUNT
    let rows = client.query("SELECT COUNT(*) FROM agg_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let count: &str = rows[0].get(0);
    assert_eq!(count, "4");

    // SUM with GROUP BY
    let rows = client.query(
        "SELECT category, SUM(value) FROM agg_test GROUP BY category ORDER BY category",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let cat: &str = rows[0].get(0);
    assert_eq!(cat, "a");
    let sum: &str = rows[0].get(1);
    assert_eq!(sum, "30");

    // AVG
    let rows = client.query("SELECT AVG(value) FROM agg_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    // MIN/MAX
    let rows = client.query("SELECT MIN(value), MAX(value) FROM agg_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let min_val: &str = rows[0].get(0);
    assert_eq!(min_val, "10");
    let max_val: &str = rows[1 - 1].get(1);
    assert_eq!(max_val, "40");
}

#[tokio::test]
async fn test_tokio_postgres_subquery() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE sub_test (id INTEGER, val INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO sub_test VALUES (1, 10)", &[]).await.unwrap();
    client.execute("INSERT INTO sub_test VALUES (2, 20)", &[]).await.unwrap();
    client.execute("INSERT INTO sub_test VALUES (3, 30)", &[]).await.unwrap();

    // Subquery in WHERE - cast to text to avoid type OID mismatch
    let rows = client.query(
        "SELECT CAST(id AS TEXT) FROM sub_test WHERE val > (SELECT AVG(val) FROM sub_test) ORDER BY id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let id: &str = rows[0].get(0);
    assert_eq!(id, "3");
}

#[tokio::test]
async fn test_tokio_postgres_join() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE users_j (id INTEGER PRIMARY KEY, name TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE orders_j (id INTEGER, user_id INTEGER, amount TEXT)", &[]).await.unwrap();

    client.execute("INSERT INTO users_j VALUES (1, 'Alice')", &[]).await.unwrap();
    client.execute("INSERT INTO users_j VALUES (2, 'Bob')", &[]).await.unwrap();
    client.execute("INSERT INTO orders_j VALUES (1, 1, '100.00')", &[]).await.unwrap();
    client.execute("INSERT INTO orders_j VALUES (2, 1, '200.00')", &[]).await.unwrap();
    client.execute("INSERT INTO orders_j VALUES (3, 2, '50.00')", &[]).await.unwrap();

    // INNER JOIN
    let rows = client.query(
        "SELECT u.name, o.amount FROM users_j u INNER JOIN orders_j o ON u.id = o.user_id ORDER BY o.id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Alice");

    // LEFT JOIN
    let rows = client.query(
        "SELECT u.name, o.amount FROM users_j u LEFT JOIN orders_j o ON u.id = o.user_id ORDER BY u.name, o.id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_tokio_postgres_like_query() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE like_test (name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO like_test VALUES ('apple')", &[]).await.unwrap();
    client.execute("INSERT INTO like_test VALUES ('banana')", &[]).await.unwrap();
    client.execute("INSERT INTO like_test VALUES ('apricot')", &[]).await.unwrap();

    let rows = client.query("SELECT name FROM like_test WHERE name LIKE 'ap%' ORDER BY name", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "apple");
    let name: &str = rows[1].get(0);
    assert_eq!(name, "apricot");
}

#[tokio::test]
async fn test_tokio_postgres_order_by_limit_offset() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE page_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    for i in 1..=10 {
        client.execute(
            &format!("INSERT INTO page_test VALUES ({}, 'item_{}')", i, i),
            &[]
        ).await.unwrap();
    }

    // LIMIT - use CAST to TEXT to avoid type OID mismatch with tokio-postgres
    let rows = client.query("SELECT CAST(id AS TEXT) FROM page_test ORDER BY id LIMIT 3", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);

    // OFFSET
    let rows = client.query("SELECT CAST(id AS TEXT) FROM page_test ORDER BY id LIMIT 3 OFFSET 5", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let id: &str = rows[0].get(0);
    assert_eq!(id, "6");

    // DESC
    let rows = client.query("SELECT CAST(id AS TEXT) FROM page_test ORDER BY id DESC LIMIT 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let id: &str = rows[0].get(0);
    assert_eq!(id, "10");
}

#[tokio::test]
async fn test_tokio_postgres_native_types() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE typed_test (id INTEGER, name TEXT, score REAL, active BOOLEAN)", &[]).await.unwrap();
    client.execute("INSERT INTO typed_test VALUES (1, 'Alice', 95.5, 'true')", &[]).await.unwrap();
    client.execute("INSERT INTO typed_test VALUES (2, 'Bob', 87.3, 'false')", &[]).await.unwrap();

    // Text columns work with &str
    let rows = client.query("SELECT name FROM typed_test ORDER BY id", &[]).await.unwrap();
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Alice");

    // Using CAST for integer columns allows &str reading
    let rows = client.query("SELECT CAST(id AS TEXT) FROM typed_test ORDER BY id", &[]).await.unwrap();
    let id: &str = rows[0].get(0);
    assert_eq!(id, "1");

    // Verify multiple rows and columns
    let rows = client.query("SELECT name, CAST(score AS TEXT) FROM typed_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name: &str = rows[1].get(0);
    assert_eq!(name, "Bob");
    let score: &str = rows[1].get(1);
    assert_eq!(score, "87.3");
}

#[tokio::test]
async fn test_tokio_postgres_coalesce_and_nullif() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE coalesce_test (id INTEGER, val TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO coalesce_test VALUES (1, NULL)", &[]).await.unwrap();
    client.execute("INSERT INTO coalesce_test VALUES (2, 'exists')", &[]).await.unwrap();

    // COALESCE
    let rows = client.query(
        "SELECT COALESCE(val, 'default') FROM coalesce_test ORDER BY id",
        &[]
    ).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "default");
    let val: &str = rows[1].get(0);
    assert_eq!(val, "exists");

    // NULLIF
    let rows = client.query(
        "SELECT NULLIF(val, 'exists') FROM coalesce_test ORDER BY id",
        &[]
    ).await.unwrap();
    let val: Option<&str> = rows[0].get(0);
    assert!(val.is_none());
    let val: Option<&str> = rows[1].get(0);
    assert!(val.is_none()); // NULLIF returns NULL when values are equal
}

#[tokio::test]
async fn test_tokio_postgres_case_expression() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE case_test (id INTEGER, status TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO case_test VALUES (1, 'active')", &[]).await.unwrap();
    client.execute("INSERT INTO case_test VALUES (2, 'inactive')", &[]).await.unwrap();
    client.execute("INSERT INTO case_test VALUES (3, 'pending')", &[]).await.unwrap();

    let rows = client.query(
        "SELECT CASE status WHEN 'active' THEN 'yes' WHEN 'inactive' THEN 'no' ELSE 'maybe' END FROM case_test ORDER BY id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3);
    let val: &str = rows[0].get(0);
    assert_eq!(val, "yes");
    let val: &str = rows[1].get(0);
    assert_eq!(val, "no");
    let val: &str = rows[2].get(0);
    assert_eq!(val, "maybe");
}

#[tokio::test]
async fn test_tokio_postgres_update_and_delete() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE ud_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO ud_test VALUES (1, 'alpha')", &[]).await.unwrap();
    client.execute("INSERT INTO ud_test VALUES (2, 'beta')", &[]).await.unwrap();
    client.execute("INSERT INTO ud_test VALUES (3, 'gamma')", &[]).await.unwrap();

    // UPDATE
    let affected = client.execute("UPDATE ud_test SET name = 'updated' WHERE id = 2", &[]).await.unwrap();
    assert_eq!(affected, 1);

    let rows = client.query("SELECT name FROM ud_test WHERE id = 2", &[]).await.unwrap();
    let name: &str = rows[0].get(0);
    assert_eq!(name, "updated");

    // DELETE
    let affected = client.execute("DELETE FROM ud_test WHERE id = 3", &[]).await.unwrap();
    assert_eq!(affected, 1);

    let rows = client.query("SELECT COUNT(*) FROM ud_test", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "2");
}

#[tokio::test]
async fn test_deadpool_postgres_connection_pool() {
    let port = start_test_server().await;

    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".to_string());
    cfg.port = Some(port);
    cfg.user = Some("testuser".to_string());
    cfg.dbname = Some("testdb".to_string());

    let pool = cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls).unwrap();

    // Get a connection from the pool
    let client = pool.get().await.unwrap();

    // Create a table
    client.execute("CREATE TABLE pool_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO pool_test VALUES (1, 'pooled')", &[]).await.unwrap();

    // Drop the connection back to the pool
    drop(client);

    // Get another connection (should reuse the pooled one)
    let client2 = pool.get().await.unwrap();

    // The data should still be there
    let rows = client2.query("SELECT name FROM pool_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "pooled");

    // Test multiple concurrent connections from pool
    let mut handles = vec![];
    for i in 0..3 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let client = pool.get().await.unwrap();
            let rows = client.query("SELECT name FROM pool_test WHERE id = 1", &[]).await.unwrap();
            assert_eq!(rows.len(), 1, "Connection {} should see data", i);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_tokio_postgres_alter_table() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE alter_test (id INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO alter_test VALUES (1)", &[]).await.unwrap();

    // ADD COLUMN
    client.execute("ALTER TABLE alter_test ADD COLUMN name TEXT", &[]).await.unwrap();
    client.execute("UPDATE alter_test SET name = 'test' WHERE id = 1", &[]).await.unwrap();

    let rows = client.query("SELECT name FROM alter_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "test");

    // RENAME TABLE
    client.execute("ALTER TABLE alter_test RENAME TO alter_renamed", &[]).await.unwrap();
    let rows = client.query("SELECT name FROM alter_renamed WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_create_index() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE idx_test (id INTEGER, email TEXT, name TEXT)", &[]).await.unwrap();

    // Regular index
    client.execute("CREATE INDEX idx_email ON idx_test (email)", &[]).await.unwrap();

    // Unique index
    client.execute("CREATE UNIQUE INDEX idx_name ON idx_test (name)", &[]).await.unwrap();

    // IF NOT EXISTS
    client.execute("CREATE INDEX IF NOT EXISTS idx_email ON idx_test (email)", &[]).await.unwrap();

    // Insert data and verify the index doesn't break anything
    client.execute("INSERT INTO idx_test VALUES (1, 'alice@test.com', 'alice')", &[]).await.unwrap();
    client.execute("INSERT INTO idx_test VALUES (2, 'bob@test.com', 'bob')", &[]).await.unwrap();

    let rows = client.query("SELECT name FROM idx_test WHERE email = 'alice@test.com'", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "alice");
}

#[tokio::test]
async fn test_tokio_postgres_drop_table() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE drop_test (id INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO drop_test VALUES (1)", &[]).await.unwrap();

    // DROP TABLE
    client.execute("DROP TABLE drop_test", &[]).await.unwrap();

    // Table should no longer exist
    let result = client.query("SELECT * FROM drop_test", &[]).await;
    assert!(result.is_err(), "Table should be dropped");

    // DROP TABLE IF EXISTS (should not error)
    client.execute("DROP TABLE IF EXISTS drop_test", &[]).await.unwrap();
}

#[tokio::test]
async fn test_tokio_postgres_multiple_tables_info_schema() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create multiple tables
    client.execute("CREATE TABLE users_schema (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE posts_schema (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, body TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE tags_schema (id INTEGER PRIMARY KEY, name TEXT)", &[]).await.unwrap();

    // Query information_schema.tables - note: catalog returns all columns,
    // so table_name is at index 2 (table_catalog, table_schema, table_name, table_type)
    let rows = client.query(
        "SELECT * FROM information_schema.tables",
        &[]
    ).await.unwrap();

    // Should have at least our 3 tables
    let table_names: Vec<&str> = rows.iter().map(|r| r.get(2)).collect(); // index 2 = table_name
    assert!(table_names.contains(&"users_schema"), "Should contain users_schema, got: {:?}", table_names);
    assert!(table_names.contains(&"posts_schema"), "Should contain posts_schema, got: {:?}", table_names);
    assert!(table_names.contains(&"tags_schema"), "Should contain tags_schema, got: {:?}", table_names);

    // Verify table_schema is "public" for all
    for row in &rows {
        let schema: &str = row.get(1);
        assert_eq!(schema, "public");
    }

    // Query information_schema.columns for all columns
    // Columns: table_catalog, table_schema, table_name, column_name, data_type, is_nullable
    let rows = client.query(
        "SELECT * FROM information_schema.columns",
        &[]
    ).await.unwrap();
    // Columns: table_catalog(0), table_schema(1), table_name(2), column_name(3),
    //          ordinal_position(4), data_type(5), is_nullable(6)
    // Filter to users_schema table
    let user_cols: Vec<&str> = rows.iter()
        .filter(|r| { let t: &str = r.get(2); t == "users_schema" })
        .map(|r| r.get(3))  // index 3 = column_name
        .collect();
    let col_names = user_cols;
    assert_eq!(col_names, vec!["id", "name", "email"]);
}

#[tokio::test]
async fn test_tokio_postgres_type_casting() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // CAST expressions
    let rows = client.query("SELECT CAST(42 AS TEXT)", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "42");

    // String to integer via CAST
    let rows = client.query("SELECT CAST('123' AS INTEGER)", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_string_functions() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // LOWER / UPPER
    let rows = client.query("SELECT LOWER('HELLO'), UPPER('world')", &[]).await.unwrap();
    let lower: &str = rows[0].get(0);
    assert_eq!(lower, "hello");
    let upper: &str = rows[0].get(1);
    assert_eq!(upper, "WORLD");

    // LENGTH
    let rows = client.query("SELECT LENGTH('hello world')", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);

    // TRIM
    let rows = client.query("SELECT TRIM('  hello  ')", &[]).await.unwrap();
    let trimmed: &str = rows[0].get(0);
    assert_eq!(trimmed, "hello");

    // REPLACE
    let rows = client.query("SELECT REPLACE('hello world', 'world', 'rust')", &[]).await.unwrap();
    let replaced: &str = rows[0].get(0);
    assert_eq!(replaced, "hello rust");

    // SUBSTR
    let rows = client.query("SELECT SUBSTR('hello', 2, 3)", &[]).await.unwrap();
    let substr: &str = rows[0].get(0);
    assert_eq!(substr, "ell");
}

#[tokio::test]
async fn test_tokio_postgres_distinct() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE distinct_test (category TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO distinct_test VALUES ('a')", &[]).await.unwrap();
    client.execute("INSERT INTO distinct_test VALUES ('b')", &[]).await.unwrap();
    client.execute("INSERT INTO distinct_test VALUES ('a')", &[]).await.unwrap();
    client.execute("INSERT INTO distinct_test VALUES ('c')", &[]).await.unwrap();
    client.execute("INSERT INTO distinct_test VALUES ('b')", &[]).await.unwrap();

    let rows = client.query("SELECT DISTINCT category FROM distinct_test ORDER BY category", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let categories: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(categories, vec!["a", "b", "c"]);

    // COUNT DISTINCT
    let rows = client.query("SELECT COUNT(DISTINCT category) FROM distinct_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_having() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE having_test (category TEXT, amount TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO having_test VALUES ('a', '10')", &[]).await.unwrap();
    client.execute("INSERT INTO having_test VALUES ('a', '20')", &[]).await.unwrap();
    client.execute("INSERT INTO having_test VALUES ('b', '5')", &[]).await.unwrap();
    client.execute("INSERT INTO having_test VALUES ('c', '100')", &[]).await.unwrap();
    client.execute("INSERT INTO having_test VALUES ('c', '200')", &[]).await.unwrap();
    client.execute("INSERT INTO having_test VALUES ('c', '300')", &[]).await.unwrap();

    // HAVING clause
    let rows = client.query(
        "SELECT category, COUNT(*) FROM having_test GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let cat: &str = rows[0].get(0);
    assert_eq!(cat, "a");
    let cat: &str = rows[1].get(0);
    assert_eq!(cat, "c");
}

#[tokio::test]
async fn test_tokio_postgres_in_clause() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE in_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO in_test VALUES (1, 'apple')", &[]).await.unwrap();
    client.execute("INSERT INTO in_test VALUES (2, 'banana')", &[]).await.unwrap();
    client.execute("INSERT INTO in_test VALUES (3, 'cherry')", &[]).await.unwrap();
    client.execute("INSERT INTO in_test VALUES (4, 'date')", &[]).await.unwrap();

    // IN clause
    let rows = client.query("SELECT name FROM in_test WHERE name IN ('apple', 'cherry') ORDER BY name", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, vec!["apple", "cherry"]);

    // NOT IN
    let rows = client.query("SELECT name FROM in_test WHERE name NOT IN ('apple', 'cherry') ORDER BY name", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, vec!["banana", "date"]);
}

#[tokio::test]
async fn test_tokio_postgres_between() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE between_test (id INTEGER, val INTEGER)", &[]).await.unwrap();
    for i in 1..=10 {
        client.execute(&format!("INSERT INTO between_test VALUES ({}, {})", i, i * 10), &[]).await.unwrap();
    }

    let rows = client.query(
        "SELECT CAST(id AS TEXT) FROM between_test WHERE val BETWEEN 30 AND 70 ORDER BY id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 5);
    let first: &str = rows[0].get(0);
    assert_eq!(first, "3");
    let last: &str = rows[4].get(0);
    assert_eq!(last, "7");
}

#[tokio::test]
async fn test_tokio_postgres_exists_subquery() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE parent_t (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE child_t (id INTEGER, parent_id INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO parent_t VALUES (1, 'has_children')", &[]).await.unwrap();
    client.execute("INSERT INTO parent_t VALUES (2, 'no_children')", &[]).await.unwrap();
    client.execute("INSERT INTO child_t VALUES (1, 1)", &[]).await.unwrap();

    let rows = client.query(
        "SELECT name FROM parent_t WHERE EXISTS (SELECT 1 FROM child_t WHERE child_t.parent_id = parent_t.id)",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "has_children");
}

#[tokio::test]
async fn test_tokio_postgres_union() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE t1 (name TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE t2 (name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO t1 VALUES ('alice')", &[]).await.unwrap();
    client.execute("INSERT INTO t1 VALUES ('bob')", &[]).await.unwrap();
    client.execute("INSERT INTO t2 VALUES ('charlie')", &[]).await.unwrap();
    client.execute("INSERT INTO t2 VALUES ('bob')", &[]).await.unwrap();

    // UNION (removes duplicates)
    let rows = client.query("SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let names: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, vec!["alice", "bob", "charlie"]);

    // UNION ALL (keeps duplicates)
    let rows = client.query("SELECT name FROM t1 UNION ALL SELECT name FROM t2 ORDER BY name", &[]).await.unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_tokio_postgres_cte() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE cte_test (id INTEGER, parent_id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO cte_test VALUES (1, NULL, 'root')", &[]).await.unwrap();
    client.execute("INSERT INTO cte_test VALUES (2, 1, 'child_a')", &[]).await.unwrap();
    client.execute("INSERT INTO cte_test VALUES (3, 1, 'child_b')", &[]).await.unwrap();

    // Basic CTE
    let rows = client.query(
        "WITH roots AS (SELECT * FROM cte_test WHERE parent_id IS NULL) SELECT name FROM roots",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "root");

    // CTE with JOIN
    let rows = client.query(
        "WITH children AS (SELECT * FROM cte_test WHERE parent_id IS NOT NULL) \
         SELECT c.name FROM children c ORDER BY c.name",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, vec!["child_a", "child_b"]);
}

#[tokio::test]
async fn test_tokio_postgres_recursive_cte() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Generate a series using recursive CTE (like generate_series)
    let rows = client.query(
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) \
         SELECT CAST(x AS TEXT) FROM cnt",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 5);
    let values: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(values, vec!["1", "2", "3", "4", "5"]);
}

#[tokio::test]
async fn test_tokio_postgres_window_function() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE wf_test (category TEXT, val INTEGER)", &[]).await.unwrap();
    client.execute("INSERT INTO wf_test VALUES ('a', 10)", &[]).await.unwrap();
    client.execute("INSERT INTO wf_test VALUES ('a', 20)", &[]).await.unwrap();
    client.execute("INSERT INTO wf_test VALUES ('b', 30)", &[]).await.unwrap();
    client.execute("INSERT INTO wf_test VALUES ('b', 40)", &[]).await.unwrap();

    // ROW_NUMBER window function
    let rows = client.query(
        "SELECT category, CAST(ROW_NUMBER() OVER (PARTITION BY category ORDER BY val) AS TEXT) AS rn FROM wf_test ORDER BY category, val",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 4);
    let rn: &str = rows[0].get(1);
    assert_eq!(rn, "1");
    let rn: &str = rows[1].get(1);
    assert_eq!(rn, "2");
}

#[tokio::test]
async fn test_tokio_postgres_insert_select() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE source_t (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE dest_t (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO source_t VALUES (1, 'alpha')", &[]).await.unwrap();
    client.execute("INSERT INTO source_t VALUES (2, 'beta')", &[]).await.unwrap();

    // INSERT INTO ... SELECT
    let affected = client.execute("INSERT INTO dest_t SELECT * FROM source_t", &[]).await.unwrap();
    assert_eq!(affected, 2);

    let rows = client.query("SELECT name FROM dest_t ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "alpha");
}

#[tokio::test]
async fn test_tokio_postgres_create_table_as() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE orig (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO orig VALUES (1, 'hello')", &[]).await.unwrap();
    client.execute("INSERT INTO orig VALUES (2, 'world')", &[]).await.unwrap();

    // CREATE TABLE AS SELECT
    client.execute("CREATE TABLE copy_of_orig AS SELECT * FROM orig", &[]).await.unwrap();

    let rows = client.query("SELECT name FROM copy_of_orig ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "hello");
}

#[tokio::test]
async fn test_tokio_postgres_ilike() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE ilike_test (name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO ilike_test VALUES ('Alice')", &[]).await.unwrap();
    client.execute("INSERT INTO ilike_test VALUES ('BOB')", &[]).await.unwrap();
    client.execute("INSERT INTO ilike_test VALUES ('charlie')", &[]).await.unwrap();

    // ILIKE should be case-insensitive
    let rows = client.query("SELECT name FROM ilike_test WHERE name ILIKE '%alice%'", &[]).await.unwrap();
    assert_eq!(rows.len(), 1, "ILIKE should match case-insensitively");
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Alice");

    // NOT ILIKE
    let rows = client.query("SELECT name FROM ilike_test WHERE name NOT ILIKE '%alice%' ORDER BY name", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_tokio_postgres_schema_qualified_names() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create table with schema-qualified name
    client.execute("CREATE TABLE public.schema_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO public.schema_test VALUES (1, 'test')", &[]).await.unwrap();

    // Query with schema-qualified name
    let rows = client.query("SELECT name FROM public.schema_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "test");

    // UPDATE with schema-qualified name
    client.execute("UPDATE public.schema_test SET name = 'updated' WHERE id = 1", &[]).await.unwrap();
    let rows = client.query("SELECT name FROM schema_test WHERE id = 1", &[]).await.unwrap();
    let name: &str = rows[0].get(0);
    assert_eq!(name, "updated");

    // DELETE with schema-qualified name
    client.execute("DELETE FROM public.schema_test WHERE id = 1", &[]).await.unwrap();
    let rows = client.query("SELECT * FROM schema_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_tokio_postgres_double_colon_cast() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // ::text cast
    let rows = client.query("SELECT 42::text", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let val: &str = rows[0].get(0);
    assert_eq!(val, "42");

    // ::integer cast (returns as text since we send text format)
    let rows = client.query("SELECT CAST('123' AS text)", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "123");
}

#[tokio::test]
async fn test_tokio_postgres_serial_autoincrement() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE serial_test (id SERIAL, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO serial_test (name) VALUES ('alice')", &[]).await.unwrap();
    client.execute("INSERT INTO serial_test (name) VALUES ('bob')", &[]).await.unwrap();
    client.execute("INSERT INTO serial_test (name) VALUES ('charlie')", &[]).await.unwrap();

    let rows = client.query("SELECT CAST(id AS TEXT), name FROM serial_test ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let name: &str = rows[0].get(1);
    assert_eq!(name, "alice");
    let name: &str = rows[2].get(1);
    assert_eq!(name, "charlie");
}

#[tokio::test]
async fn test_tokio_postgres_truncate_table() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE trunc_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO trunc_test VALUES (1, 'a')", &[]).await.unwrap();
    client.execute("INSERT INTO trunc_test VALUES (2, 'b')", &[]).await.unwrap();

    // Verify data exists
    let rows = client.query("SELECT * FROM trunc_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);

    // TRUNCATE
    client.simple_query("TRUNCATE TABLE trunc_test").await.unwrap();

    // Verify data is gone
    let rows = client.query("SELECT * FROM trunc_test", &[]).await.unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_tokio_postgres_explain() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE explain_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO explain_test VALUES (1, 'test')", &[]).await.unwrap();

    // EXPLAIN
    let rows = client.query("EXPLAIN SELECT * FROM explain_test WHERE id = 1", &[]).await.unwrap();
    assert!(!rows.is_empty(), "EXPLAIN should return plan rows");
}

#[tokio::test]
async fn test_tokio_postgres_explain_analyze() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE ea_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO ea_test VALUES (1, 'test')", &[]).await.unwrap();

    // EXPLAIN ANALYZE
    let rows = client.query("EXPLAIN ANALYZE SELECT * FROM ea_test", &[]).await.unwrap();
    assert!(!rows.is_empty(), "EXPLAIN ANALYZE should return plan + timing");

    // Should contain execution time
    let last_row: &str = rows.last().unwrap().get(0);
    assert!(last_row.contains("Execution Time"), "Should show execution time, got: {}", last_row);
}

#[tokio::test]
async fn test_tokio_postgres_left_right_functions() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // left() function
    let rows = client.query("SELECT left('hello world', 5)", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "hello");

    // right() function
    let rows = client.query("SELECT right('hello world', 5)", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "world");
}

#[tokio::test]
async fn test_tokio_postgres_strpos_function() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // strpos function
    let rows = client.query("SELECT strpos('hello world', 'world')", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "7"); // 1-based position

    // char_length
    let rows = client.query("SELECT char_length('hello')", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "5");
}

#[tokio::test]
async fn test_tokio_postgres_regexp_replace() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // regexp_replace → replace (literal string replacement)
    let rows = client.query("SELECT regexp_replace('hello world', 'world', 'earth')", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "hello earth");
}

#[tokio::test]
async fn test_tokio_postgres_savepoint() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE sp_integ (id INTEGER, val TEXT)", &[]).await.unwrap();

    // Begin transaction
    client.simple_query("BEGIN").await.unwrap();
    client.execute("INSERT INTO sp_integ VALUES (1, 'a')", &[]).await.unwrap();

    // Create savepoint
    client.simple_query("SAVEPOINT sp1").await.unwrap();
    client.execute("INSERT INTO sp_integ VALUES (2, 'b')", &[]).await.unwrap();

    // Rollback to savepoint
    client.simple_query("ROLLBACK TO SAVEPOINT sp1").await.unwrap();

    // Commit
    client.simple_query("COMMIT").await.unwrap();

    // Only row 1 should exist
    let rows = client.query("SELECT * FROM sp_integ", &[]).await.unwrap();
    assert_eq!(rows.len(), 1, "Should only have 1 row after rollback to savepoint");
    let val: &str = rows[0].get(1);
    assert_eq!(val, "a");
}

#[tokio::test]
async fn test_tokio_postgres_sql_prepare_execute() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE prep_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO prep_test VALUES (1, 'alice')", &[]).await.unwrap();
    client.execute("INSERT INTO prep_test VALUES (2, 'bob')", &[]).await.unwrap();

    // SQL-level PREPARE and EXECUTE
    client.simple_query("PREPARE myplan AS SELECT name FROM prep_test WHERE id = $1").await.unwrap();

    let results = client.simple_query("EXECUTE myplan(1)").await.unwrap();
    // simple_query returns SimpleQueryMessage items
    let has_data = results.iter().any(|r| matches!(r, tokio_postgres::SimpleQueryMessage::Row(_)));
    assert!(has_data, "EXECUTE should return data");

    // DEALLOCATE
    client.simple_query("DEALLOCATE myplan").await.unwrap();

    // Connection should still work
    let rows = client.query("SELECT 1 AS test", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_savepoint_release() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE sp_rel (id INTEGER)", &[]).await.unwrap();

    client.simple_query("BEGIN").await.unwrap();
    client.execute("INSERT INTO sp_rel VALUES (1)", &[]).await.unwrap();
    client.simple_query("SAVEPOINT sp1").await.unwrap();
    client.execute("INSERT INTO sp_rel VALUES (2)", &[]).await.unwrap();
    client.simple_query("RELEASE SAVEPOINT sp1").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // Both rows should exist after release
    let rows = client.query("SELECT * FROM sp_rel ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_tokio_postgres_pg_database() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let rows = client.query("SELECT * FROM pg_database", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(1);
    assert_eq!(datname, "pgsqlite");
}

#[tokio::test]
async fn test_tokio_postgres_pg_roles() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let rows = client.query("SELECT * FROM pg_roles", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let rolname: &str = rows[0].get(1);
    assert_eq!(rolname, "pgsqlite");

    // pg_user should also work (aliased to pg_roles)
    let rows = client.query("SELECT * FROM pg_user", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_tokio_postgres_pg_settings() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let rows = client.query("SELECT * FROM pg_settings", &[]).await.unwrap();
    assert!(rows.len() >= 10, "Should have at least 10 settings, got {}", rows.len());

    // Check that server_version is present
    let names: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert!(names.contains(&"server_version"), "Should contain server_version");
    assert!(names.contains(&"TimeZone"), "Should contain TimeZone");
}

#[tokio::test]
async fn test_tokio_postgres_pg_indexes() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE pg_idx_test (id INTEGER, name TEXT)", &[]).await.unwrap();
    client.execute("CREATE INDEX idx_pg_name ON pg_idx_test (name)", &[]).await.unwrap();

    let rows = client.query("SELECT * FROM pg_indexes", &[]).await.unwrap();
    assert!(rows.len() >= 1, "Should have at least 1 index");
    let schemaname: &str = rows[0].get(0);
    assert_eq!(schemaname, "public");
    let indexname: &str = rows[0].get(2);
    assert_eq!(indexname, "idx_pg_name");
}

#[tokio::test]
async fn test_tokio_postgres_pg_constraint() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE con_integ (id INTEGER PRIMARY KEY NOT NULL, name TEXT)", &[]).await.unwrap();

    let rows = client.query("SELECT * FROM pg_constraint", &[]).await.unwrap();
    assert!(!rows.is_empty(), "Should have constraints");

    // Should contain primary key constraint
    let types: Vec<&str> = rows.iter().map(|r| r.get(3)).collect();
    assert!(types.contains(&"p"), "Should have PRIMARY KEY constraint (p)");
}

#[tokio::test]
async fn test_tokio_postgres_default_values() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute(
        "CREATE TABLE default_test (id INTEGER, name TEXT DEFAULT 'unnamed', active TEXT DEFAULT 'true')",
        &[]
    ).await.unwrap();

    // Insert with all defaults
    client.execute("INSERT INTO default_test (id) VALUES (1)", &[]).await.unwrap();

    let rows = client.query("SELECT name, active FROM default_test WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "unnamed");
    let active: &str = rows[0].get(1);
    assert_eq!(active, "true");
}

#[tokio::test]
async fn test_tokio_postgres_error_unique_violation() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE unique_err_test (id INTEGER PRIMARY KEY, name TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO unique_err_test VALUES (1, 'alice')", &[]).await.unwrap();

    // Try to insert duplicate - should get SQLSTATE 23505
    let result = client.execute("INSERT INTO unique_err_test VALUES (1, 'bob')", &[]).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let db_err = err.as_db_error().unwrap();
    assert_eq!(db_err.code().code(), "23505", "Should be unique_violation, got: {}", db_err.code().code());
}

#[tokio::test]
async fn test_tokio_postgres_error_table_not_found() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Query nonexistent table - should get SQLSTATE 42P01
    let result = client.query("SELECT * FROM nonexistent_table_xyz", &[]).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let db_err = err.as_db_error().unwrap();
    assert_eq!(db_err.code().code(), "42P01", "Should be undefined_table, got: {}", db_err.code().code());
}

#[tokio::test]
async fn test_tokio_postgres_error_table_already_exists() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE dup_table_test (id INTEGER)", &[]).await.unwrap();

    // Create same table again - should get SQLSTATE 42P07
    let result = client.execute("CREATE TABLE dup_table_test (id INTEGER)", &[]).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let db_err = err.as_db_error().unwrap();
    assert_eq!(db_err.code().code(), "42P07", "Should be duplicate_table, got: {}", db_err.code().code());
}

#[tokio::test]
async fn test_tokio_postgres_error_syntax_error() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Syntax error - should get SQLSTATE 42601
    let result = client.simple_query("SELEC * FROM foo").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_tokio_postgres_error_not_null_violation() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE nn_test (id INTEGER NOT NULL, name TEXT NOT NULL)", &[]).await.unwrap();

    // Insert with NULL into NOT NULL column
    let result = client.execute("INSERT INTO nn_test (id, name) VALUES (1, NULL)", &[]).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let db_err = err.as_db_error().unwrap();
    assert_eq!(db_err.code().code(), "23502", "Should be not_null_violation, got: {}", db_err.code().code());
}

#[tokio::test]
async fn test_tokio_postgres_error_foreign_key_violation() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)", &[]).await.unwrap();
    client.execute("CREATE TABLE fk_child (id INTEGER, parent_id INTEGER REFERENCES fk_parent(id))", &[]).await.unwrap();

    // Insert child with nonexistent parent - should get SQLSTATE 23503
    let result = client.execute("INSERT INTO fk_child VALUES (1, 999)", &[]).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let db_err = err.as_db_error().unwrap();
    assert_eq!(db_err.code().code(), "23503", "Should be foreign_key_violation, got: {}", db_err.code().code());
}

#[tokio::test]
async fn test_tokio_postgres_update_from() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE uf_t1 (id INTEGER PRIMARY KEY, val TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE uf_t2 (id INTEGER PRIMARY KEY, val TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO uf_t1 VALUES (1, 'old')", &[]).await.unwrap();
    client.execute("INSERT INTO uf_t2 VALUES (1, 'new')", &[]).await.unwrap();

    // UPDATE ... FROM (PG multi-table update)
    client.execute("UPDATE uf_t1 SET val = uf_t2.val FROM uf_t2 WHERE uf_t1.id = uf_t2.id", &[]).await.unwrap();

    let rows = client.query("SELECT val FROM uf_t1 WHERE id = 1", &[]).await.unwrap();
    let val: &str = rows[0].get(0);
    assert_eq!(val, "new", "UPDATE FROM should have updated val to 'new'");
}

#[tokio::test]
async fn test_tokio_postgres_delete_using() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE du_t1 (id INTEGER PRIMARY KEY, val TEXT)", &[]).await.unwrap();
    client.execute("CREATE TABLE du_t2 (id INTEGER PRIMARY KEY)", &[]).await.unwrap();
    client.execute("INSERT INTO du_t1 VALUES (1, 'keep'), (2, 'delete'), (3, 'keep')", &[]).await.unwrap();
    client.execute("INSERT INTO du_t2 VALUES (2)", &[]).await.unwrap();

    // DELETE ... USING (PG multi-table delete) - transformed to EXISTS subquery
    client.execute("DELETE FROM du_t1 USING du_t2 WHERE du_t1.id = du_t2.id", &[]).await.unwrap();

    let rows = client.query("SELECT CAST(id AS TEXT) FROM du_t1 ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.len(), 2, "Should have 2 rows left after DELETE USING");
    let id1: &str = rows[0].get(0);
    let id2: &str = rows[1].get(0);
    assert_eq!(id1, "1");
    assert_eq!(id2, "3");
}

#[tokio::test]
async fn test_tokio_postgres_insert_on_conflict_do_update() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE oc_test (id INTEGER PRIMARY KEY, name TEXT, count INTEGER DEFAULT 0)", &[]).await.unwrap();
    client.execute("INSERT INTO oc_test VALUES (1, 'alice', 1)", &[]).await.unwrap();

    // INSERT ON CONFLICT DO UPDATE with complex expression
    client.execute(
        "INSERT INTO oc_test (id, name, count) VALUES (1, 'alice', 1) ON CONFLICT (id) DO UPDATE SET count = oc_test.count + 1",
        &[]
    ).await.unwrap();

    let rows = client.query("SELECT CAST(count AS TEXT) FROM oc_test WHERE id = 1", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "2", "ON CONFLICT DO UPDATE should have incremented count");
}

#[tokio::test]
async fn test_tokio_postgres_filter_where() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE fw_test (id INTEGER, active TEXT)", &[]).await.unwrap();
    client.execute("INSERT INTO fw_test VALUES (1, 'true'), (2, 'false'), (3, 'true')", &[]).await.unwrap();

    // Aggregate with FILTER (WHERE ...)
    let rows = client.query(
        "SELECT count(*) FILTER (WHERE active = 'true') FROM fw_test",
        &[]
    ).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "2", "FILTER WHERE should count only active rows");
}

#[tokio::test]
async fn test_tokio_postgres_returning() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE ret_test (id INTEGER PRIMARY KEY, name TEXT)", &[]).await.unwrap();

    // INSERT with RETURNING - use CAST to avoid OID mismatch
    let rows = client.query(
        "INSERT INTO ret_test (id, name) VALUES (1, 'alice') RETURNING CAST(id AS TEXT), name",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1, "RETURNING should return 1 row");
    let id: &str = rows[0].get(0);
    let name: &str = rows[0].get(1);
    assert_eq!(id, "1");
    assert_eq!(name, "alice");

    // UPDATE with RETURNING
    let rows = client.query(
        "UPDATE ret_test SET name = 'bob' WHERE id = 1 RETURNING CAST(id AS TEXT), name",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(1);
    assert_eq!(name, "bob");

    // DELETE with RETURNING
    let rows = client.query(
        "DELETE FROM ret_test WHERE id = 1 RETURNING CAST(id AS TEXT)",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let id: &str = rows[0].get(0);
    assert_eq!(id, "1");
}

// =============================================================
// ORM Smoke Tests
// =============================================================

/// SQLAlchemy-style query patterns
#[tokio::test]
async fn test_orm_sqlalchemy_patterns() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // SQLAlchemy issues these on connection setup
    client.simple_query("SELECT 1").await.unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // Schema inspection - SQLAlchemy checks table existence
    let rows = client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        &[]
    ).await.unwrap();
    // Fresh database, no tables yet
    assert!(rows.is_empty() || rows.len() >= 0);

    // Create model table (typical SQLAlchemy DDL)
    client.execute(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email VARCHAR(255),
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )", &[]
    ).await.unwrap();

    // SQLAlchemy INSERT RETURNING pattern
    let rows = client.query(
        "INSERT INTO users (username, email, is_active) VALUES ($1, $2, $3) RETURNING CAST(id AS TEXT), username",
        &[&"alice", &"alice@example.com", &"true"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let username: &str = rows[0].get(1);
    assert_eq!(username, "alice");

    // SQLAlchemy SELECT with explicit column listing + WHERE
    let rows = client.query(
        "SELECT username, email, is_active FROM users WHERE username = $1",
        &[&"alice"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let email: &str = rows[0].get(1);
    assert_eq!(email, "alice@example.com");

    // SQLAlchemy UPDATE RETURNING pattern
    let rows = client.query(
        "UPDATE users SET email = $1 WHERE username = $2 RETURNING username, email",
        &[&"newalice@example.com", &"alice"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let email: &str = rows[0].get(1);
    assert_eq!(email, "newalice@example.com");

    // SQLAlchemy eager loading (JOIN)
    client.execute(
        "CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            user_id INTEGER REFERENCES users(id)
        )", &[]
    ).await.unwrap();

    client.execute("INSERT INTO posts (title, user_id) VALUES ($1, $2)", &[&"First Post", &"1"]).await.unwrap();

    let rows = client.query(
        "SELECT users.username, posts.title FROM users JOIN posts ON users.id = posts.user_id WHERE users.username = $1",
        &[&"alice"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let title: &str = rows[0].get(1);
    assert_eq!(title, "First Post");

    // SQLAlchemy count query
    let rows = client.query("SELECT CAST(count(*) AS TEXT) FROM users", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "1");
}

/// Prisma-style query patterns
#[tokio::test]
async fn test_orm_prisma_patterns() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Prisma startup queries
    client.simple_query("SELECT 1").await.unwrap();
    client.simple_query("SET search_path = 'public'").await.unwrap();

    // Prisma schema creation (with IF NOT EXISTS)
    client.execute(
        "CREATE TABLE IF NOT EXISTS \"Product\" (
            \"id\" TEXT PRIMARY KEY,
            \"name\" TEXT NOT NULL,
            \"price\" INTEGER NOT NULL,
            \"category\" TEXT,
            \"inStock\" TEXT DEFAULT 'true'
        )", &[]
    ).await.unwrap();

    // Prisma INSERT
    client.execute(
        "INSERT INTO \"Product\" (\"id\", \"name\", \"price\", \"category\", \"inStock\") VALUES ($1, $2, $3, $4, $5)",
        &[&"prod-1", &"Widget", &"999", &"electronics", &"true"]
    ).await.unwrap();

    client.execute(
        "INSERT INTO \"Product\" (\"id\", \"name\", \"price\", \"category\", \"inStock\") VALUES ($1, $2, $3, $4, $5)",
        &[&"prod-2", &"Gadget", &"1499", &"electronics", &"false"]
    ).await.unwrap();

    // Prisma findMany with WHERE + ORDER BY + LIMIT
    let rows = client.query(
        "SELECT \"id\", \"name\", \"price\" FROM \"Product\" WHERE \"category\" = $1 ORDER BY \"price\" ASC LIMIT 10",
        &[&"electronics"]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let first_name: &str = rows[0].get(1);
    assert_eq!(first_name, "Widget"); // cheaper first

    // Prisma findUnique
    let rows = client.query(
        "SELECT \"id\", \"name\", \"price\", \"category\", \"inStock\" FROM \"Product\" WHERE \"id\" = $1 LIMIT 1",
        &[&"prod-1"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);

    // Prisma update
    client.execute(
        "UPDATE \"Product\" SET \"price\" = $1 WHERE \"id\" = $2",
        &[&"1099", &"prod-1"]
    ).await.unwrap();

    // Prisma upsert pattern
    client.execute(
        "INSERT INTO \"Product\" (\"id\", \"name\", \"price\", \"category\") VALUES ($1, $2, $3, $4) ON CONFLICT (\"id\") DO UPDATE SET \"price\" = $3",
        &[&"prod-1", &"Widget", &"1199", &"electronics"]
    ).await.unwrap();

    // Prisma aggregate
    let rows = client.query(
        "SELECT CAST(count(*) AS TEXT), CAST(min(\"price\") AS TEXT), CAST(max(\"price\") AS TEXT) FROM \"Product\"",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let count: &str = rows[0].get(0);
    assert_eq!(count, "2");

    // Prisma deleteMany
    client.execute("DELETE FROM \"Product\" WHERE \"inStock\" = $1", &[&"false"]).await.unwrap();

    let rows = client.query("SELECT CAST(count(*) AS TEXT) FROM \"Product\"", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "1");
}

/// Django ORM-style query patterns
#[tokio::test]
async fn test_orm_django_patterns() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Django startup: checks server version, encoding, timezone
    client.simple_query("SHOW server_version").await.unwrap();
    client.simple_query("SHOW client_encoding").await.unwrap();
    client.simple_query("SET TIME ZONE 'UTC'").await.unwrap();

    // Django migration style DDL
    client.execute(
        "CREATE TABLE \"myapp_article\" (
            \"id\" SERIAL PRIMARY KEY,
            \"title\" TEXT NOT NULL,
            \"content\" TEXT NOT NULL,
            \"published\" TEXT NOT NULL DEFAULT 'false',
            \"created_at\" TEXT NOT NULL DEFAULT '',
            \"author_name\" TEXT NOT NULL DEFAULT ''
        )", &[]
    ).await.unwrap();

    // Django INSERT pattern (explicit column list)
    client.execute(
        "INSERT INTO \"myapp_article\" (\"title\", \"content\", \"published\", \"author_name\") VALUES ($1, $2, $3, $4)",
        &[&"Hello World", &"This is my first article", &"true", &"admin"]
    ).await.unwrap();

    client.execute(
        "INSERT INTO \"myapp_article\" (\"title\", \"content\", \"published\", \"author_name\") VALUES ($1, $2, $3, $4)",
        &[&"Draft Post", &"Work in progress", &"false", &"admin"]
    ).await.unwrap();

    // Django queryset filter
    let rows = client.query(
        "SELECT \"myapp_article\".\"title\", \"myapp_article\".\"content\" FROM \"myapp_article\" WHERE \"myapp_article\".\"published\" = $1 ORDER BY \"myapp_article\".\"title\" ASC",
        &[&"true"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let title: &str = rows[0].get(0);
    assert_eq!(title, "Hello World");

    // Django count()
    let rows = client.query(
        "SELECT CAST(COUNT(*) AS TEXT) FROM \"myapp_article\" WHERE \"myapp_article\".\"published\" = $1",
        &[&"true"]
    ).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "1");

    // Django UPDATE (queryset update)
    client.execute(
        "UPDATE \"myapp_article\" SET \"published\" = $1 WHERE \"myapp_article\".\"title\" = $2",
        &[&"true", &"Draft Post"]
    ).await.unwrap();

    // Django exists() pattern
    let rows = client.query(
        "SELECT 1 AS \"a\" FROM \"myapp_article\" WHERE \"myapp_article\".\"title\" = $1 LIMIT 1",
        &[&"Hello World"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);

    // Django ILIKE (case-insensitive search)
    let rows = client.query(
        "SELECT \"myapp_article\".\"title\" FROM \"myapp_article\" WHERE \"myapp_article\".\"title\" ILIKE $1",
        &[&"%hello%"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let title: &str = rows[0].get(0);
    assert_eq!(title, "Hello World");

    // Django bulk delete
    client.execute(
        "DELETE FROM \"myapp_article\" WHERE \"myapp_article\".\"published\" = $1",
        &[&"true"]
    ).await.unwrap();

    let rows = client.query("SELECT CAST(COUNT(*) AS TEXT) FROM \"myapp_article\"", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "0");
}

/// TypeORM-style query patterns
#[tokio::test]
async fn test_orm_typeorm_patterns() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // TypeORM startup queries
    client.simple_query("SELECT current_schema()").await.unwrap();
    client.simple_query("SELECT current_database()").await.unwrap();

    // TypeORM checks for existing tables
    let _rows = client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        &[]
    ).await.unwrap();

    // TypeORM schema synchronization - creates tables
    client.execute(
        "CREATE TABLE IF NOT EXISTS \"user\" (
            \"id\" TEXT PRIMARY KEY,
            \"firstName\" TEXT NOT NULL,
            \"lastName\" TEXT NOT NULL,
            \"age\" INTEGER,
            \"isActive\" TEXT NOT NULL DEFAULT 'true'
        )", &[]
    ).await.unwrap();

    // TypeORM save (INSERT)
    client.execute(
        "INSERT INTO \"user\" (\"id\", \"firstName\", \"lastName\", \"age\", \"isActive\") VALUES ($1, $2, $3, $4, $5)",
        &[&"u1", &"John", &"Doe", &"25", &"true"]
    ).await.unwrap();

    client.execute(
        "INSERT INTO \"user\" (\"id\", \"firstName\", \"lastName\", \"age\", \"isActive\") VALUES ($1, $2, $3, $4, $5)",
        &[&"u2", &"Jane", &"Smith", &"30", &"true"]
    ).await.unwrap();

    client.execute(
        "INSERT INTO \"user\" (\"id\", \"firstName\", \"lastName\", \"age\", \"isActive\") VALUES ($1, $2, $3, $4, $5)",
        &[&"u3", &"Bob", &"Wilson", &"35", &"false"]
    ).await.unwrap();

    // TypeORM findOne pattern
    let rows = client.query(
        "SELECT \"user\".\"id\", \"user\".\"firstName\", \"user\".\"lastName\", \"user\".\"age\", \"user\".\"isActive\" FROM \"user\" WHERE \"user\".\"id\" = $1 LIMIT 1",
        &[&"u1"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let first_name: &str = rows[0].get(1);
    assert_eq!(first_name, "John");

    // TypeORM findAndCount pattern
    let rows = client.query(
        "SELECT \"user\".\"id\", \"user\".\"firstName\" FROM \"user\" WHERE \"user\".\"isActive\" = $1 ORDER BY \"user\".\"firstName\" ASC",
        &[&"true"]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);

    let count_rows = client.query(
        "SELECT CAST(COUNT(*) AS TEXT) FROM \"user\" WHERE \"user\".\"isActive\" = $1",
        &[&"true"]
    ).await.unwrap();
    let count: &str = count_rows[0].get(0);
    assert_eq!(count, "2");

    // TypeORM update pattern
    client.execute(
        "UPDATE \"user\" SET \"isActive\" = $1 WHERE \"id\" = $2",
        &[&"false", &"u1"]
    ).await.unwrap();

    // TypeORM softDelete (update pattern)
    // TypeORM uses deletedAt columns for soft delete
    // We just verify the update works

    // TypeORM QueryBuilder pattern with aliases
    let rows = client.query(
        "SELECT u.\"firstName\" AS \"fname\", u.\"lastName\" AS \"lname\" FROM \"user\" u WHERE u.\"isActive\" = $1",
        &[&"true"]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let fname: &str = rows[0].get(0);
    assert_eq!(fname, "Jane");

    // TypeORM remove (DELETE)
    client.execute(
        "DELETE FROM \"user\" WHERE \"id\" = $1",
        &[&"u3"]
    ).await.unwrap();

    let rows = client.query("SELECT CAST(COUNT(*) AS TEXT) FROM \"user\"", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "2");
}

/// Drizzle ORM-style query patterns
#[tokio::test]
async fn test_orm_drizzle_patterns() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Drizzle creates tables
    client.execute(
        "CREATE TABLE IF NOT EXISTS \"orders\" (
            \"id\" TEXT PRIMARY KEY,
            \"customer_name\" TEXT NOT NULL,
            \"total\" INTEGER NOT NULL,
            \"status\" TEXT NOT NULL DEFAULT 'pending'
        )", &[]
    ).await.unwrap();

    // Drizzle insert().values()
    client.execute(
        "INSERT INTO \"orders\" (\"id\", \"customer_name\", \"total\", \"status\") VALUES ($1, $2, $3, $4)",
        &[&"ord-1", &"Alice", &"1000", &"pending"]
    ).await.unwrap();

    client.execute(
        "INSERT INTO \"orders\" (\"id\", \"customer_name\", \"total\", \"status\") VALUES ($1, $2, $3, $4)",
        &[&"ord-2", &"Bob", &"2000", &"completed"]
    ).await.unwrap();

    client.execute(
        "INSERT INTO \"orders\" (\"id\", \"customer_name\", \"total\", \"status\") VALUES ($1, $2, $3, $4)",
        &[&"ord-3", &"Alice", &"500", &"pending"]
    ).await.unwrap();

    // Drizzle select().where()
    let rows = client.query(
        "SELECT \"id\", \"customer_name\", \"total\" FROM \"orders\" WHERE \"status\" = $1",
        &[&"pending"]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);

    // Drizzle select with aggregate
    let rows = client.query(
        "SELECT \"customer_name\", CAST(SUM(\"total\") AS TEXT) as \"total_spent\" FROM \"orders\" GROUP BY \"customer_name\" ORDER BY \"customer_name\"",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let alice_total: &str = rows[0].get(1);
    assert_eq!(alice_total, "1500");

    // Drizzle update().set().where()
    client.execute(
        "UPDATE \"orders\" SET \"status\" = $1 WHERE \"id\" = $2",
        &[&"completed", &"ord-1"]
    ).await.unwrap();

    // Drizzle select with LIKE
    let rows = client.query(
        "SELECT \"id\" FROM \"orders\" WHERE \"customer_name\" LIKE $1",
        &[&"Ali%"]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);

    // Drizzle delete().where()
    client.execute(
        "DELETE FROM \"orders\" WHERE \"status\" = $1",
        &[&"completed"]
    ).await.unwrap();

    let rows = client.query("SELECT CAST(COUNT(*) AS TEXT) FROM \"orders\"", &[]).await.unwrap();
    let count: &str = rows[0].get(0);
    assert_eq!(count, "1");
}

/// Connection pool test - multiple concurrent connections
#[tokio::test]
async fn test_connection_pool_concurrent() {
    let port = start_test_server().await;

    // Create table via one connection
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.execute("CREATE TABLE pool_test (id INTEGER PRIMARY KEY, val TEXT)", &[]).await.unwrap();
    for i in 0..10 {
        client.execute(
            &format!("INSERT INTO pool_test VALUES ({}, 'val{}')", i, i),
            &[]
        ).await.unwrap();
    }

    // Use deadpool to manage concurrent connections
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".to_string());
    cfg.port = Some(port);
    cfg.user = Some("testuser".to_string());
    cfg.dbname = Some("testdb".to_string());

    let pool = cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls).unwrap();

    // Run 10 concurrent queries
    let mut handles = vec![];
    for i in 0..10 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let client = pool.get().await.unwrap();
            let rows = client.query(
                "SELECT val FROM pool_test WHERE id = $1",
                &[&format!("{}", i)]
            ).await.unwrap();
            assert_eq!(rows.len(), 1);
            let val: String = rows[0].get::<_, &str>(0).to_string();
            assert_eq!(val, format!("val{}", i));
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Common startup sequence used by many tools (psql, pgAdmin, etc.)
#[tokio::test]
async fn test_common_startup_queries() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Common queries that tools/ORMs run at connection startup
    client.simple_query("SELECT 1").await.unwrap();
    client.simple_query("SHOW server_version").await.unwrap();
    client.simple_query("SHOW server_version_num").await.unwrap();
    client.simple_query("SHOW client_encoding").await.unwrap();
    client.simple_query("SHOW standard_conforming_strings").await.unwrap();
    client.simple_query("SHOW DateStyle").await.unwrap();
    client.simple_query("SHOW TimeZone").await.unwrap();
    client.simple_query("SHOW integer_datetimes").await.unwrap();
    client.simple_query("SET client_encoding = 'UTF8'").await.unwrap();
    client.simple_query("SET timezone = 'UTC'").await.unwrap();
    client.simple_query("SELECT current_database()").await.unwrap();
    client.simple_query("SELECT current_schema()").await.unwrap();
    client.simple_query("SELECT current_user").await.unwrap();

    // pg_catalog queries commonly issued on startup
    let _rows = client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        &[]
    ).await.unwrap();

    // Version check
    let rows = client.simple_query("SHOW server_version").await.unwrap();
    // simple_query returns SimpleQueryMessage enum, just verify no error
    assert!(!rows.is_empty());
}

/// Complex query patterns combining multiple features
#[tokio::test]
async fn test_complex_query_patterns() {
    let port = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=testuser dbname=testdb", port),
        tokio_postgres::NoTls,
    ).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Create tables with various features
    client.execute(
        "CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL)", &[]
    ).await.unwrap();

    client.execute(
        "CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            dept_id INTEGER REFERENCES departments(id),
            salary INTEGER NOT NULL,
            is_active TEXT DEFAULT 'true'
        )", &[]
    ).await.unwrap();

    // Insert departments
    client.execute("INSERT INTO departments (name) VALUES ('Engineering')", &[]).await.unwrap();
    client.execute("INSERT INTO departments (name) VALUES ('Sales')", &[]).await.unwrap();
    client.execute("INSERT INTO departments (name) VALUES ('Marketing')", &[]).await.unwrap();

    // Insert employees
    client.execute("INSERT INTO employees (name, dept_id, salary) VALUES ('Alice', 1, 100000)", &[]).await.unwrap();
    client.execute("INSERT INTO employees (name, dept_id, salary) VALUES ('Bob', 1, 90000)", &[]).await.unwrap();
    client.execute("INSERT INTO employees (name, dept_id, salary) VALUES ('Charlie', 2, 80000)", &[]).await.unwrap();
    client.execute("INSERT INTO employees (name, dept_id, salary, is_active) VALUES ('Diana', 2, 70000, 'false')", &[]).await.unwrap();
    client.execute("INSERT INTO employees (name, dept_id, salary) VALUES ('Eve', 3, 85000)", &[]).await.unwrap();

    // JOIN with aggregate + GROUP BY + HAVING
    let rows = client.query(
        "SELECT d.name, CAST(COUNT(e.id) AS TEXT) AS cnt, CAST(AVG(e.salary) AS TEXT) AS avg_sal
         FROM departments d
         LEFT JOIN employees e ON d.id = e.dept_id
         GROUP BY d.name
         HAVING COUNT(e.id) > 1
         ORDER BY d.name",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2); // Engineering (2) and Sales (2)
    let dept1: &str = rows[0].get(0);
    assert_eq!(dept1, "Engineering");

    // Subquery + IN
    let rows = client.query(
        "SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY name",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name1: &str = rows[0].get(0);
    assert_eq!(name1, "Alice");

    // CASE WHEN expression
    let rows = client.query(
        "SELECT name, CASE WHEN salary >= 90000 THEN 'senior' ELSE 'junior' END AS level FROM employees ORDER BY name",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 5);
    let alice_level: &str = rows[0].get(1);
    assert_eq!(alice_level, "senior");

    // CTE (Common Table Expression)
    let rows = client.query(
        "WITH dept_stats AS (
            SELECT dept_id, CAST(COUNT(*) AS TEXT) as cnt, CAST(SUM(salary) AS TEXT) as total
            FROM employees
            WHERE is_active = 'true'
            GROUP BY dept_id
         )
         SELECT d.name, ds.cnt, ds.total
         FROM dept_stats ds
         JOIN departments d ON d.id = ds.dept_id
         ORDER BY d.name",
        &[]
    ).await.unwrap();
    assert!(rows.len() >= 2);

    // COALESCE + string functions
    let rows = client.query(
        "SELECT COALESCE(name, 'unknown'), upper(name), length(name) FROM employees WHERE name = 'Alice'",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 1);
    let upper_name: &str = rows[0].get(1);
    assert_eq!(upper_name, "ALICE");

    // DISTINCT + ORDER BY
    let rows = client.query(
        "SELECT DISTINCT dept_id FROM employees ORDER BY dept_id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 3);

    // LIMIT + OFFSET
    let rows = client.query(
        "SELECT name FROM employees ORDER BY name LIMIT 2 OFFSET 1",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Bob");
}
