# @pgsqlite/sdk

Node.js SDK for [pgsqlite](../../README.md) — a PostgreSQL wire-protocol server backed by SQLite.

Zero runtime dependencies. Manages the pgsqlite server lifecycle and provides connection info for any PostgreSQL client or ORM.

## Install

```bash
npm install @pgsqlite/sdk
```

## Quick Start

```typescript
import { PgSqlite } from '@pgsqlite/sdk';
import pg from 'pg';

const server = new PgSqlite({ database: ':memory:' });
await server.start();

const client = new pg.Client(server.getPgConfig());
await client.connect();

await client.query('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
await client.query("INSERT INTO users VALUES (1, 'Alice')");

const { rows } = await client.query('SELECT * FROM users');
console.log(rows); // [{ id: 1, name: 'Alice' }]

await client.end();
await server.stop();
```

## API

### `new PgSqlite(options?)`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `database` | `string` | `':memory:'` | SQLite database path or `':memory:'` |
| `host` | `string` | `'127.0.0.1'` | Address to bind to |
| `port` | `number` | `0` | Port (0 = auto-assign) |
| `binaryPath` | `string` | — | Explicit path to pgsqlite binary |
| `startupTimeout` | `number` | `10000` | Startup timeout in ms |
| `shutdownTimeout` | `number` | `5000` | Shutdown timeout in ms |

### Instance Methods

- **`start()`** — Start the server. Resolves when ready for connections.
- **`stop()`** — Gracefully stop the server.
- **`getConnectionString()`** — Returns `postgresql://pgsqlite:password@host:port/pgsqlite`
- **`getPgConfig()`** — Returns `{ host, port, user, password, database, ssl }` for pg clients.
- **`port`** — The actual bound port number.
- **`started`** — Whether the server is running.

### Binary Resolution

The SDK finds the pgsqlite binary in this order:

1. `binaryPath` option
2. `PGSQLITE_BINARY` environment variable
3. Walk up from cwd looking for `target/release/pgsqlite` or `target/debug/pgsqlite`
4. `which pgsqlite` (PATH lookup)

## Testing Helpers

```typescript
import { createTestServer, createTestHelper, usePgSqlite } from '@pgsqlite/sdk/testing';

// One-shot: creates and starts a server
const { connectionString, config, port, stop } = await createTestServer();
// ... use it ...
await stop();

// Lifecycle helper for beforeAll/afterAll
const helper = createTestHelper();
beforeAll(() => helper.start());
afterAll(() => helper.stop());

// Auto-hook (registers beforeAll/afterAll automatically)
const pg = usePgSqlite();
test('query', async () => {
  const client = new Client(pg.config);
  // ...
});
```

## ORM Integration

Works with any PostgreSQL client. Pass `getConnectionString()` or `getPgConfig()`:

- **node-postgres**: `new pg.Client(server.getPgConfig())`
- **Prisma**: Set `DATABASE_URL` to `server.getConnectionString()`
- **Drizzle**: `drizzle(new pg.Pool(server.getPgConfig()))`
- **Knex**: `knex({ connection: server.getConnectionString() })`

## Development

```bash
# Build the Rust binary first
cargo build --release

# Install, build, and test the SDK
cd sdk/node
npm install
npm run build
npm test
```

## License

MIT
