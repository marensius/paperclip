import { companies, heartbeatRuns, type Db } from "@paperclipai/db";
import { and, eq, sql, type SQL } from "drizzle-orm";
import { logger } from "../../middleware/logger.js";

const GLOBAL_RUN_ADMISSION_LOCK_KEY = "paperclip:heartbeat-run-admission";
const DEFAULT_GLOBAL_QUEUE_SCAN_LIMIT = 200;
const DEFAULT_GLOBAL_BACKGROUND_STARVATION_MS = 900_000;

function rejectInvalidLimit(value: unknown, envName: string, allowZero: boolean): never {
  logger.warn(
    { value, env: envName },
    `${envName} is not a valid concurrency limit; refusing to start with an ambiguous cap`,
  );
  const expected = allowZero
    ? "a non-negative integer (0 or unset disables the limit)"
    : "a positive integer";
  throw new Error(
    `${envName} must be ${expected}; got ${JSON.stringify(value)}. ` +
      "Fix or unset the variable before starting the server.",
  );
}

function parseDisabledCap(value: unknown, envName: string): number {
  if (value === null || value === undefined) return 0;
  let numeric: number;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") return 0;
    numeric = Number(trimmed);
  } else if (typeof value === "number") {
    numeric = value;
  } else {
    return rejectInvalidLimit(value, envName, true);
  }
  if (!Number.isFinite(numeric) || numeric < 0) {
    return rejectInvalidLimit(value, envName, true);
  }
  return Math.floor(numeric);
}

function parsePositiveInteger(
  value: unknown,
  envName: string,
  fallback: number,
): number {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "string" && value.trim() === "") return fallback;
  const parsed =
    typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isFinite(parsed) || parsed <= 0 || Math.floor(parsed) !== parsed) {
    return rejectInvalidLimit(value, envName, false);
  }
  return parsed;
}

export function resolveGlobalMaxConcurrentRuns(
  value: unknown = process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS,
): number {
  return parseDisabledCap(value, "PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS");
}

export function resolveGlobalMaxConcurrentRunsPerCompany(
  value: unknown = process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY,
): number {
  return parseDisabledCap(
    value,
    "PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY",
  );
}

export function resolveGlobalQueueScanLimit(
  value: unknown = process.env.PAPERCLIP_GLOBAL_QUEUE_SCAN_LIMIT,
): number {
  return parsePositiveInteger(
    value,
    "PAPERCLIP_GLOBAL_QUEUE_SCAN_LIMIT",
    DEFAULT_GLOBAL_QUEUE_SCAN_LIMIT,
  );
}

export function resolveGlobalBackgroundStarvationMs(
  value: unknown = process.env.PAPERCLIP_GLOBAL_BACKGROUND_STARVATION_MS,
): number {
  return parsePositiveInteger(
    value,
    "PAPERCLIP_GLOBAL_BACKGROUND_STARVATION_MS",
    DEFAULT_GLOBAL_BACKGROUND_STARVATION_MS,
  );
}

export interface GlobalRunAdmissionOptions {
  companyId?: string;
  companyLimit?: number;
}

export async function admitGlobalRunSlot(
  tx: Db,
  limit: number,
  options: GlobalRunAdmissionOptions = {},
): Promise<boolean> {
  const companyLimit = options.companyLimit ?? 0;
  const companyId = options.companyId ?? null;
  const globalEnabled = limit > 0;
  const companyEnabled = companyLimit > 0 && Boolean(companyId);
  if (!globalEnabled && !companyEnabled) return true;

  const [lock] = await tx.execute<{ locked: boolean }>(sql`
    select pg_try_advisory_xact_lock(
      hashtextextended(${GLOBAL_RUN_ADMISSION_LOCK_KEY}, 0)
    ) as locked
  `);
  if (!lock?.locked) return false;

  if (globalEnabled) {
    const [row] = await tx
      .select({ count: sql<number>`count(*)::int` })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.status, "running"));
    if (Number(row?.count ?? 0) >= limit) return false;
  }

  if (companyEnabled && companyId) {
    const [row] = await tx
      .select({ count: sql<number>`count(*)::int` })
      .from(heartbeatRuns)
      .where(
        and(
          eq(heartbeatRuns.status, "running"),
          eq(heartbeatRuns.companyId, companyId),
        ),
      );
    if (Number(row?.count ?? 0) >= companyLimit) return false;
  }

  return true;
}

export interface GlobalQueuedRunCandidate {
  companyId: string;
  agentId: string;
  createdAt: Date;
  isManual: boolean;
}

export function globalQueueCandidateQuery(
  cutoff: Date | null,
  limit: number,
): SQL {
  return sql`
    select distinct on (r.company_id, r.agent_id)
      r.company_id, r.agent_id, r.created_at,
      (r.invocation_source = 'on_demand' and r.trigger_detail = 'manual') as is_manual
    from heartbeat_runs r
    join companies c on c.id = r.company_id
    where r.status = 'queued' and c.status = 'active'
      and (${cutoff}::timestamptz is null or r.created_at >= ${cutoff}::timestamptz)
    order by r.company_id, r.agent_id, r.created_at asc
    limit ${limit}
  `;
}

export async function listGlobalQueuedRunCandidates(
  db: Db,
  options: { cutoff: Date | null; limit: number },
): Promise<GlobalQueuedRunCandidate[]> {
  const rows = await db.execute<{
    company_id: string;
    agent_id: string;
    created_at: Date | string;
    is_manual: boolean;
  }>(globalQueueCandidateQuery(options.cutoff, options.limit));

  return rows.map((row) => ({
    companyId: row.company_id,
    agentId: row.agent_id,
    createdAt: new Date(row.created_at),
    isManual: Boolean(row.is_manual),
  }));
}

export interface GlobalQueueBacklogStats {
  queuedCount: number;
  oldestQueuedAt: Date | null;
  oldestQueuedAgeMs: number | null;
}

export async function readGlobalQueueBacklogStats(
  db: Db,
  cutoff: Date | null,
): Promise<GlobalQueueBacklogStats> {
  const [row] = await db.execute<{
    queued_count: number;
    oldest_queued_at: Date | string | null;
  }>(sql`
    select count(*)::int as queued_count, min(r.created_at) as oldest_queued_at
    from heartbeat_runs r
    join companies c on c.id = r.company_id
    where r.status = 'queued' and c.status = 'active'
      and (${cutoff}::timestamptz is null or r.created_at >= ${cutoff}::timestamptz)
  `);

  const queuedCount = Number(row?.queued_count ?? 0);
  const oldestQueuedAt = row?.oldest_queued_at
    ? new Date(row.oldest_queued_at)
    : null;
  return {
    queuedCount,
    oldestQueuedAt,
    oldestQueuedAgeMs: oldestQueuedAt
      ? Math.max(0, Date.now() - oldestQueuedAt.getTime())
      : null,
  };
}
