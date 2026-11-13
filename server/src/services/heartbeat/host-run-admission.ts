import { environments, heartbeatRuns, type Db } from "@paperclipai/db";
import { and, eq, sql } from "drizzle-orm";

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

  const [row] = await tx
    .select({ count: sql<number>`count(*)::int` })
    .from(heartbeatRuns)
    .where(
      and(
        eq(heartbeatRuns.status, "running"),
        sql`${heartbeatRuns.contextSnapshot} ->> ${RUN_ADMISSION_HOST_KEY} = ${hostKey}`,
      ),
    );
  return Number(row?.count ?? 0) < limit;
}
