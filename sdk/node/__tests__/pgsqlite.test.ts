import { describe, it, expect, afterEach } from 'vitest';
import { PgSqlite } from '../src/pgsqlite.js';

describe('PgSqlite', () => {
  let server: PgSqlite | null = null;

  afterEach(async () => {
    if (server?.started) {
      await server.stop();
    }
    server = null;
  });

  it('should start and stop', async () => {
    server = new PgSqlite({ database: ':memory:' });
    await server.start();

    expect(server.started).toBe(true);
    expect(server.port).toBeGreaterThan(0);

    await server.stop();
    expect(server.started).toBe(false);
  });

  it('should assign a random port when port is 0', async () => {
    server = new PgSqlite({ database: ':memory:', port: 0 });
    await server.start();

    expect(server.port).toBeGreaterThan(0);
    expect(server.port).not.toBe(5432);
  });

  it('should return a valid connection string', async () => {
    server = new PgSqlite({ database: ':memory:' });
    await server.start();

    const connStr = server.getConnectionString();
    expect(connStr).toMatch(
      /^postgresql:\/\/pgsqlite:password@127\.0\.0\.1:\d+\/pgsqlite$/
    );
  });

  it('should return valid pg config', async () => {
    server = new PgSqlite({ database: ':memory:' });
    await server.start();

    const config = server.getPgConfig();
    expect(config.host).toBe('127.0.0.1');
    expect(config.port).toBe(server.port);
    expect(config.user).toBe('pgsqlite');
    expect(config.password).toBe('password');
    expect(config.database).toBe('pgsqlite');
    expect(config.ssl).toBe(false);
  });

  it('should throw if accessing port before start', () => {
    server = new PgSqlite({ database: ':memory:' });
    expect(() => server!.port).toThrow('Server not started');
  });

  it('should throw if started twice', async () => {
    server = new PgSqlite({ database: ':memory:' });
    await server.start();
    await expect(server.start()).rejects.toThrow('already started');
  });

  it('should be safe to stop when not started', async () => {
    server = new PgSqlite({ database: ':memory:' });
    await server.stop(); // Should not throw
  });
});
