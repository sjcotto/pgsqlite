export class PgSqliteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PgSqliteError';
  }
}

export class BinaryNotFoundError extends PgSqliteError {
  constructor(searchedPaths: string[]) {
    super(
      `pgsqlite binary not found. Searched:\n${searchedPaths.map((p) => `  - ${p}`).join('\n')}\n\nSet PGSQLITE_BINARY env var or pass binaryPath option.`
    );
    this.name = 'BinaryNotFoundError';
  }
}

export class StartupError extends PgSqliteError {
  constructor(message: string, public readonly stderr?: string) {
    super(message);
    this.name = 'StartupError';
  }
}
