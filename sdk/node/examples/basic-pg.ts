/**
 * Basic example using node-postgres (pg) with pgsqlite.
 *
 * Run: npx tsx examples/basic-pg.ts
 */
import { PgSqlite } from '../src/index.js';
import pg from 'pg';

const { Client } = pg;

async function main() {
  const server = new PgSqlite({ database: ':memory:' });
  await server.start();
  console.log(`pgsqlite started on port ${server.port}`);
  console.log(`Connection string: ${server.getConnectionString()}`);

  const client = new Client(server.getPgConfig());
  await client.connect();

  // Create a table
  await client.query(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE
    )
  `);

  // Insert data
  await client.query(
    'INSERT INTO users (id, name, email) VALUES ($1, $2, $3)',
    [1, 'Alice', 'alice@example.com']
  );
  await client.query(
    'INSERT INTO users (id, name, email) VALUES ($1, $2, $3)',
    [2, 'Bob', 'bob@example.com']
  );

  // Query data
  const result = await client.query('SELECT * FROM users ORDER BY id');
  console.log('Users:', result.rows);

  await client.end();
  await server.stop();
  console.log('Server stopped.');
}

main().catch(console.error);
