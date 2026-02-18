import { spawn, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { EventEmitter } from 'node:events';
import { resolveBinary } from './binary.js';
import { PgSqliteError, StartupError } from './errors.js';
import type { PgSqliteOptions, ConnectionInfo } from './types.js';

async function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      if (addr && typeof addr === 'object') {
        const port = addr.port;
        server.close(() => resolve(port));
      } else {
        server.close(() => reject(new Error('Failed to get port')));
      }
    });
    server.on('error', reject);
  });
}

export class PgSqlite extends EventEmitter {
  private opts: Required<PgSqliteOptions>;
  private process: ChildProcess | null = null;
  private _port: number | null = null;
  private _started = false;
  private stderrOutput = '';

  constructor(options: PgSqliteOptions = {}) {
    super();
    this.opts = {
      database: options.database ?? ':memory:',
      host: options.host ?? '127.0.0.1',
      port: options.port ?? 0,
      binaryPath: options.binaryPath ?? '',
      startupTimeout: options.startupTimeout ?? 10_000,
      shutdownTimeout: options.shutdownTimeout ?? 5_000,
    };
  }

  get port(): number {
    if (this._port === null) {
      throw new PgSqliteError('Server not started. Call start() first.');
    }
    return this._port;
  }

  get started(): boolean {
    return this._started;
  }

  getConnectionString(): string {
    return `postgresql://pgsqlite:password@${this.opts.host}:${this.port}/pgsqlite`;
  }

  getPgConfig(): ConnectionInfo {
    return {
      host: this.opts.host,
      port: this.port,
      user: 'pgsqlite',
      password: 'password',
      database: 'pgsqlite',
      ssl: false,
    };
  }

  async start(): Promise<void> {
    if (this._started) {
      throw new PgSqliteError('Server already started.');
    }

    const binaryPath = resolveBinary(this.opts.binaryPath || undefined);
    const port = this.opts.port === 0 ? await findFreePort() : this.opts.port;
    const addr = `${this.opts.host}:${port}`;

    const env: Record<string, string> = {
      ...process.env as Record<string, string>,
      PGSQLITE_ADDR: addr,
      PGSQLITE_DB: this.opts.database,
    };

    this.process = spawn(binaryPath, [], {
      env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    this.process.stderr?.on('data', (chunk: Buffer) => {
      this.stderrOutput += chunk.toString();
    });

    return new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.kill();
        reject(
          new StartupError(
            `pgsqlite failed to start within ${this.opts.startupTimeout}ms`,
            this.stderrOutput
          )
        );
      }, this.opts.startupTimeout);

      this.process!.on('error', (err) => {
        clearTimeout(timeout);
        reject(new StartupError(`Failed to spawn pgsqlite: ${err.message}`));
      });

      this.process!.on('exit', (code) => {
        if (!this._started) {
          clearTimeout(timeout);
          reject(
            new StartupError(
              `pgsqlite exited with code ${code} during startup`,
              this.stderrOutput
            )
          );
        } else {
          this._started = false;
          this._port = null;
          this.emit('exit', code);
        }
      });

      let stdout = '';
      this.process!.stdout?.on('data', (chunk: Buffer) => {
        stdout += chunk.toString();
        const match = stdout.match(/Listening on:\s*(.+)/);
        if (match) {
          clearTimeout(timeout);
          const listenAddr = match[1].trim();
          const portMatch = listenAddr.match(/:(\d+)$/);
          if (portMatch) {
            this._port = parseInt(portMatch[1], 10);
          } else {
            this._port = port;
          }
          this._started = true;
          resolve();
        }
      });
    });
  }

  async stop(): Promise<void> {
    if (!this._started || !this.process) {
      return;
    }

    return new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        this.kill();
        resolve();
      }, this.opts.shutdownTimeout);

      this.process!.on('exit', () => {
        clearTimeout(timeout);
        this._started = false;
        this._port = null;
        this.process = null;
        resolve();
      });

      this.process!.kill('SIGTERM');
    });
  }

  private kill(): void {
    try {
      this.process?.kill('SIGKILL');
    } catch {
      // Process may already be dead
    }
    this.process = null;
    this._started = false;
    this._port = null;
  }
}
