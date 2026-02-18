/**
 * Example showing how to use pgsqlite with Prisma.
 *
 * This is a conceptual example - you'd need to set up a Prisma schema
 * and generate the client first.
 *
 * 1. Set DATABASE_URL in .env to the pgsqlite connection string
 * 2. Run: npx prisma db push
 * 3. Run: npx prisma generate
 * 4. Use the Prisma client as usual
 */
import { PgSqlite } from '../src/index.js';

async function main() {
  const server = new PgSqlite({ database: ':memory:' });
  await server.start();

  // Set the DATABASE_URL for Prisma
  const connectionString = server.getConnectionString();
  console.log(`Set DATABASE_URL=${connectionString}`);
  console.log('Then run: npx prisma db push && npx prisma generate');

  // Example Prisma usage (requires generated client):
  //
  // import { PrismaClient } from '@prisma/client';
  // const prisma = new PrismaClient({
  //   datasources: { db: { url: connectionString } },
  // });
  //
  // await prisma.user.create({ data: { name: 'Alice' } });
  // const users = await prisma.user.findMany();
  // console.log(users);
  //
  // await prisma.$disconnect();

  await server.stop();
}

main().catch(console.error);
