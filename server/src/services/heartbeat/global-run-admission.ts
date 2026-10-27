import { heartbeatRuns, type Db } from "@paperclipai/db";
import { eq, sql } from "drizzle-orm";

const GLOBAL_RUN_ADMISSION_LOCK_KEY = "paperclip:heartbeat-run-admission";

export function resolveGlobalMaxConcurrentRuns(
  value: unknown = process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS,
): number {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return parsed;
}

export async function admitGlobalRunSlot(
  tx: Db,
  limit: number,
): Promise<boolean> {
  if (limit <= 0) return true;

  const [lock] = await tx.execute<{ locked: boolean }>(sql`
    select pg_try_advisory_xact_lock(
      hashtextextended(${GLOBAL_RUN_ADMISSION_LOCK_KEY}, 0)
    ) as locked
  `);
  if (!lock?.locked) return false;

  const [row] = await tx
    .select({ count: sql<number>`count(*)::int` })
    .from(heartbeatRuns)
    .where(eq(heartbeatRuns.status, "running"));
  return Number(row?.count ?? 0) < limit;
}
