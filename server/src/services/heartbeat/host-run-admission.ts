import { environments, heartbeatRuns, type Db } from "@paperclipai/db";
import { eq, sql } from "drizzle-orm";

const HOST_RUN_ADMISSION_LOCK_PREFIX =
  "paperclip:heartbeat-run-admission:host";

export const RUN_ADMISSION_HOST_KEY = "paperclipAdmissionHostKey";

export function resolveMaxConcurrentRunsPerHost(
  value: unknown = process.env.PAPERCLIP_MAX_CONCURRENT_RUNS_PER_HOST,
): number {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return parsed;
}

export function resolveMaxQueuedRunsPerHost(
  value: unknown = process.env.PAPERCLIP_MAX_QUEUED_RUNS_PER_HOST,
): number {
  if (value === null || value === undefined || value === "") return 0;
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return parsed;
}

export function resolveEnvironmentHostKey(
  environment:
    | {
        id: string;
        driver: string;
        config: Record<string, unknown> | null;
      }
    | null
    | undefined,
): string | null {
  if (!environment) return null;
  if (environment.driver === "ssh") {
    const host = environment.config?.host;
    if (typeof host === "string" && host.trim().length > 0) {
      return `ssh:${host.trim().toLowerCase()}`;
    }
    return `env:${environment.id}`;
  }
  if (environment.driver === "local") return "local";
  return `env:${environment.id}`;
}

export async function resolveRunAdmissionHostKey(
  db: Db,
  input: {
    environmentId: string | null | undefined;
  },
): Promise<string | null> {
  if (!input.environmentId) return null;
  const [environment] = await db
    .select({
      id: environments.id,
      driver: environments.driver,
      config: environments.config,
    })
    .from(environments)
    .where(eq(environments.id, input.environmentId))
    .limit(1);
  return resolveEnvironmentHostKey(environment ?? null);
}

export function readRunAdmissionHostKey(
  contextSnapshot: Record<string, unknown> | null | undefined,
): string | null {
  const value = contextSnapshot?.[RUN_ADMISSION_HOST_KEY];
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

export function withRunAdmissionHostKey(
  contextSnapshot: Record<string, unknown> | null | undefined,
  hostKey: string,
): Record<string, unknown> {
  return {
    ...(contextSnapshot ?? {}),
    [RUN_ADMISSION_HOST_KEY]: hostKey,
  };
}

export async function admitHostRunSlot(
  tx: Db,
  limit: number,
  hostKey: string | null | undefined,
): Promise<boolean> {
  if (limit <= 0 || !hostKey) return true;

  const [lock] = await tx.execute<{ locked: boolean }>(sql`
    select pg_try_advisory_xact_lock(
      hashtextextended(${`${HOST_RUN_ADMISSION_LOCK_PREFIX}:${hostKey}`}, 0)
    ) as locked
  `);
  if (!lock?.locked) return false;

  const { running } = await countHostRunStates(tx, hostKey);
  return running < limit;
}

/**
 * Counts the admission-relevant heartbeat runs persisted for one host key.
 * `queued` deliberately includes `scheduled_retry` so a retry can never slip
 * past the queued budget while its predecessor settles.
 */
export async function countHostRunStates(
  tx: Db,
  hostKey: string,
): Promise<{ running: number; queued: number }> {
  const [row] = await tx
    .select({
      running: sql<number>`count(*) filter (where ${heartbeatRuns.status} = 'running')::int`,
      queued: sql<number>`count(*) filter (where ${heartbeatRuns.status} in ('queued', 'scheduled_retry'))::int`,
    })
    .from(heartbeatRuns)
    .where(
      sql`${heartbeatRuns.contextSnapshot} ->> ${RUN_ADMISSION_HOST_KEY} = ${hostKey}`,
    );
  return {
    running: Number(row?.running ?? 0),
    queued: Number(row?.queued ?? 0),
  };
}

/**
 * Enqueue-time admission for the per-host queued-run depth cap. Refuses while
 * the host's queued budget (`PAPERCLIP_MAX_QUEUED_RUNS_PER_HOST`) is full so
 * the caller can park the wake instead of stacking another queued run.
 *
 * Uses the same advisory xact lock prefix as {@link admitHostRunSlot} so queue
 * admission and claim admission serialize on one host. The lock is try-only on
 * purpose: the enqueue transaction may already hold the issue lock while a
 * claim transaction holds the host lock and waits for that issue lock, so a
 * blocking acquisition would deadlock; a busy lock conservatively refuses and
 * the wake is retried later.
 */
export async function admitHostQueuedRun(
  tx: Db,
  input: {
    maxRunning: number;
    maxQueued: number;
    hostKey: string | null | undefined;
  },
): Promise<boolean> {
  const { maxRunning, maxQueued, hostKey } = input;
  if (maxQueued <= 0 || !hostKey) return true;

  const [lock] = await tx.execute<{ locked: boolean }>(sql`
    select pg_try_advisory_xact_lock(
      hashtextextended(${`${HOST_RUN_ADMISSION_LOCK_PREFIX}:${hostKey}`}, 0)
    ) as locked
  `);
  if (!lock?.locked) return false;

  const { running, queued } = await countHostRunStates(tx, hostKey);
  if (queued >= maxQueued) return false;
  if (maxRunning > 0 && running + queued >= maxRunning + maxQueued) return false;
  return true;
}
