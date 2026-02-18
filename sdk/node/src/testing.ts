import { PgSqlite } from './pgsqlite.js';
import type { PgSqliteOptions, ConnectionInfo } from './types.js';

export interface TestServer {
  server: PgSqlite;
  connectionString: string;
  config: ConnectionInfo;
  port: number;
  stop: () => Promise<void>;
}

/**
 * Create a test server with in-memory SQLite.
 * Returns a started server ready for connections.
 */
export async function createTestServer(
  options?: PgSqliteOptions
): Promise<TestServer> {
  const server = new PgSqlite({
    database: ':memory:',
    ...options,
  });
  await server.start();
  return {
    server,
    connectionString: server.getConnectionString(),
    config: server.getPgConfig(),
    port: server.port,
    stop: () => server.stop(),
  };
}

export interface TestHelper {
  connectionString: string;
  config: ConnectionInfo;
  port: number;
  start: () => Promise<void>;
  stop: () => Promise<void>;
}

/**
 * Create a test helper for use in beforeAll/afterAll hooks.
 */
export function createTestHelper(options?: PgSqliteOptions): TestHelper {
  const server = new PgSqlite({
    database: ':memory:',
    ...options,
  });

  return {
    get connectionString() {
      return server.getConnectionString();
    },
    get config() {
      return server.getPgConfig();
    },
    get port() {
      return server.port;
    },
    start: () => server.start(),
    stop: () => server.stop(),
  };
}

/**
 * Vitest/Jest-style hook that starts a server before all tests
 * and stops it after all tests.
 *
 * Usage:
 *   const helper = usePgSqlite();
 *   test('...', async () => {
 *     const client = new Client(helper.config);
 *   });
 */
export function usePgSqlite(
  options?: PgSqliteOptions,
  hooks?: {
    beforeAll: (fn: () => Promise<void>) => void;
    afterAll: (fn: () => Promise<void>) => void;
  }
): TestHelper {
  const helper = createTestHelper(options);

  const { beforeAll: ba, afterAll: aa } = hooks ?? {
    beforeAll: (globalThis as any).beforeAll,
    afterAll: (globalThis as any).afterAll,
  };

  if (typeof ba === 'function' && typeof aa === 'function') {
    ba(() => helper.start());
    aa(() => helper.stop());
  }

  return helper;
}

export { PgSqlite } from './pgsqlite.js';
export type { PgSqliteOptions, ConnectionInfo } from './types.js';
