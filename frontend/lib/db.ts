/**
 * Pool de conexões pg singleton — reusado entre requests do Next.
 * Em dev (HMR) o módulo é recarregado, por isso o cache em globalThis.
 */
import { Pool, QueryResultRow } from 'pg';

const globalForPool = globalThis as unknown as { _susforgePool?: Pool };

function getPool(): Pool {
  if (!globalForPool._susforgePool) {
    globalForPool._susforgePool = new Pool({
      host: process.env.SUSFORGE_DB_HOST ?? 'localhost',
      port: Number(process.env.SUSFORGE_DB_PORT ?? 5432),
      user: process.env.POSTGRES_USER ?? 'susforge',
      password: process.env.POSTGRES_PASSWORD ?? 'susforge_change_me',
      database: process.env.POSTGRES_DB ?? 'susforge',
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });
  }
  return globalForPool._susforgePool;
}

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: ReadonlyArray<unknown>,
): Promise<T[]> {
  const pool = getPool();
  const res = await pool.query<T>(text, params as unknown[] | undefined);
  return res.rows;
}

export async function ping(): Promise<boolean> {
  try {
    const rows = await query<{ ok: number }>('SELECT 1 AS ok');
    return rows[0]?.ok === 1;
  } catch {
    return false;
  }
}
