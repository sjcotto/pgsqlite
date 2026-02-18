import { accessSync, constants } from 'node:fs';
import { resolve, join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { BinaryNotFoundError } from './errors.js';

function isExecutable(filePath: string): boolean {
  try {
    accessSync(filePath, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function findInRepoWalk(startDir: string): string | null {
  let dir = startDir;
  const root = resolve('/');
  while (dir !== root) {
    for (const candidate of [
      join(dir, 'target', 'release', 'pgsqlite'),
      join(dir, 'target', 'debug', 'pgsqlite'),
    ]) {
      if (isExecutable(candidate)) return candidate;
    }
    const parent = resolve(dir, '..');
    if (parent === dir) break;
    dir = parent;
  }
  return null;
}

function findInPath(): string | null {
  try {
    const result = execFileSync('which', ['pgsqlite'], {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe'],
    }).trim();
    return result || null;
  } catch {
    return null;
  }
}

export function resolveBinary(explicitPath?: string): string {
  const searched: string[] = [];

  // Level 1: Explicit path
  if (explicitPath) {
    const resolved = resolve(explicitPath);
    if (isExecutable(resolved)) return resolved;
    searched.push(`explicit: ${resolved}`);
  }

  // Level 2: PGSQLITE_BINARY env var
  const envPath = process.env.PGSQLITE_BINARY;
  if (envPath) {
    const resolved = resolve(envPath);
    if (isExecutable(resolved)) return resolved;
    searched.push(`env PGSQLITE_BINARY: ${resolved}`);
  }

  // Level 3: Walk up from cwd looking for target/release/pgsqlite
  const repoResult = findInRepoWalk(process.cwd());
  if (repoResult) return repoResult;
  searched.push(`repo walk from: ${process.cwd()}`);

  // Level 4: PATH lookup
  const pathResult = findInPath();
  if (pathResult) return pathResult;
  searched.push('PATH lookup (which pgsqlite)');

  throw new BinaryNotFoundError(searched);
}
