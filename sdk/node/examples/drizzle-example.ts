/**
 * Example showing how to use pgsqlite with Drizzle ORM.
 *
 * This is a conceptual example - you'd need drizzle-orm and
 * drizzle-orm/node-postgres installed.
 */
import { PgSqlite } from '../src/index.js';

async function main() {
  const server = new PgSqlite({ database: ':memory:' });
  await server.start();

  console.log(`pgsqlite started on port ${server.port}`);
  console.log(`Connection string: ${server.getConnectionString()}`);

  // Example Drizzle usage (requires drizzle-orm):
  //
  // import { drizzle } from 'drizzle-orm/node-postgres';
  // import { pgTable, serial, text } from 'drizzle-orm/pg-core';
  // import pg from 'pg';
  //
  // const users = pgTable('users', {
  //   id: serial('id').primaryKey(),
  //   name: text('name').notNull(),
  // });
  //
  // const pool = new pg.Pool(server.getPgConfig());
  // const db = drizzle(pool);
  //
  // await db.insert(users).values({ name: 'Alice' });
  // const allUsers = await db.select().from(users);
  // console.log(allUsers);
  //
  // await pool.end();

  await server.stop();
}

main().catch(console.error);
