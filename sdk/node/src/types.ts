export interface PgSqliteOptions {
  /** Path to the SQLite database file, or ':memory:' for in-memory */
  database?: string;
  /** Address to bind to (default: '127.0.0.1') */
  host?: string;
  /** Port to listen on (default: 0 for auto-assign) */
  port?: number;
  /** Explicit path to the pgsqlite binary */
  binaryPath?: string;
  /** Startup timeout in milliseconds (default: 10000) */
  startupTimeout?: number;
  /** Graceful shutdown timeout in milliseconds (default: 5000) */
  shutdownTimeout?: number;
}

export interface ConnectionInfo {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl: false;
}
