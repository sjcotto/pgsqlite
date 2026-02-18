import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  createTestServer,
  createTestHelper,
  usePgSqlite,
} from '../src/testing.js';

describe('createTestServer', () => {
  it('should create and return a running server', async () => {
    const { server, connectionString, config, port, stop } =
      await createTestServer();

    expect(server.started).toBe(true);
    expect(port).toBeGreaterThan(0);
    expect(connectionString).toContain(`${port}`);
    expect(config.port).toBe(port);

    await stop();
    expect(server.started).toBe(false);
  });
});

describe('createTestHelper', () => {
  it('should create a helper with start/stop lifecycle', async () => {
    const helper = createTestHelper();

    await helper.start();
    expect(helper.port).toBeGreaterThan(0);
    expect(helper.connectionString).toContain(`${helper.port}`);

    await helper.stop();
  });
});

describe('usePgSqlite', () => {
  let startCalled = false;
  let stopCalled = false;

  const helper = usePgSqlite({}, {
    beforeAll: (fn) => {
      startCalled = true;
      // Don't actually call fn - just verify registration
    },
    afterAll: (fn) => {
      stopCalled = true;
    },
  });

  it('should register beforeAll and afterAll hooks', () => {
    expect(startCalled).toBe(true);
    expect(stopCalled).toBe(true);
  });
});

describe('usePgSqlite with real hooks', () => {
  const helper = usePgSqlite(undefined, { beforeAll, afterAll });

  it('should have a running server', () => {
    expect(helper.port).toBeGreaterThan(0);
    expect(helper.connectionString).toContain(`${helper.port}`);
    expect(helper.config.host).toBe('127.0.0.1');
  });
});
