use pgsqlite::server::PgServer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("pgsqlite=info".parse().unwrap()))
        .init();

    let addr = std::env::var("PGSQLITE_ADDR").unwrap_or_else(|_| "127.0.0.1:5432".to_string());
    let db_path = std::env::var("PGSQLITE_DB").unwrap_or_else(|_| "pgsqlite.db".to_string());

    println!("pgsqlite - PostgreSQL wire protocol server backed by SQLite");
    println!("Database: {}", db_path);

    let server = PgServer::new(&addr, &db_path).expect("Failed to create server");
    server.run().await.expect("Server error");
}
