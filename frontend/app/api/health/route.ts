import { NextResponse } from 'next/server';
import { ping } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  const dbOk = await ping();
  return NextResponse.json(
    {
      status: dbOk ? 'healthy' : 'degraded',
      checks: { postgres: dbOk },
      service: 'susforge-frontend',
      time: new Date().toISOString(),
    },
    { status: dbOk ? 200 : 503 },
  );
}
