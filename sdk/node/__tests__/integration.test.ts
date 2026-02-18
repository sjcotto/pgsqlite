import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pg from 'pg';
import { PgSqlite } from '../src/pgsqlite.js';

const { Client } = pg;

describe('Integration with pg driver', () => {
  let server: PgSqlite;

  beforeAll(async () => {
    server = new PgSqlite({ database: ':memory:' });
    await server.start();
  });

  afterAll(async () => {
    await server.stop();
  });

  it('should connect and run a simple query', async () => {
    const client = new Client(server.getPgConfig());
    await client.connect();

    const result = await client.query('SELECT 1 AS num');
    // pgsqlite/SQLite returns integers as strings since SQLite is dynamically typed
    expect(Number(result.rows[0].num)).toBe(1);

    await client.end();
  });

  it('should create a table and insert/select data', async () => {
    const client = new Client(server.getPgConfig());
    await client.connect();

    await client.query(
      'CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name TEXT)'
    );
    await client.query("INSERT INTO test_users (id, name) VALUES (1, 'Alice')");
    await client.query("INSERT INTO test_users (id, name) VALUES (2, 'Bob')");

    const result = await client.query(
      'SELECT * FROM test_users ORDER BY id'
    );
    expect(result.rows).toHaveLength(2);
    expect(result.rows[0].name).toBe('Alice');
    expect(result.rows[1].name).toBe('Bob');

    await client.end();
  });

  it('should work with parameterized queries', async () => {
    const client = new Client(server.getPgConfig());
    await client.connect();

    await client.query(
      'CREATE TABLE IF NOT EXISTS test_params (id INTEGER PRIMARY KEY, value TEXT)'
    );
    await client.query('INSERT INTO test_params (id, value) VALUES ($1, $2)', [
      1,
      'hello',
    ]);

    const result = await client.query(
      'SELECT value FROM test_params WHERE id = $1',
      [1]
    );
    expect(result.rows[0].value).toBe('hello');

    await client.end();
  });
});
