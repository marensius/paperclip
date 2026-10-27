import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents,
  agentWakeupRequests,
  companies,
  createDb,
  heartbeatRuns,
  type Db,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.ts";
import {
  admitGlobalRunSlot,
  resolveGlobalMaxConcurrentRuns,
} from "../services/heartbeat/global-run-admission.ts";
import { runningProcesses } from "../adapters/index.ts";

const mockAdapterExecute = vi.hoisted(() =>
  vi.fn(async () => ({
    exitCode: 0,
    signal: null,
    timedOut: false,
    errorMessage: null,
    summary: "Global run admission test run.",
    provider: "test",
    model: "test-model",
  })),
);

vi.mock("../adapters/index.ts", async () => {
  const actual = await vi.importActual<typeof import("../adapters/index.ts")>(
    "../adapters/index.ts",
  );
  return {
    ...actual,
    getServerAdapter: vi.fn(() => ({
      supportsLocalAgentJwt: false,
      execute: mockAdapterExecute,
    })),
  };
});

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const externalDatabaseUrl =
  process.env.PAPERCLIP_TEST_DATABASE_URL?.trim() || null;
const canUseDatabase = Boolean(externalDatabaseUrl) || embeddedPostgresSupport.supported;
const describeDatabase = canUseDatabase ? describe : describe.skip;

if (!canUseDatabase) {
  console.warn(
    `Skipping embedded Postgres global run admission tests on this host: ${
      embeddedPostgresSupport.reason ?? "unsupported environment"
    }`,
  );
}

describe("resolveGlobalMaxConcurrentRuns", () => {
  it("treats unset, invalid, and non-positive values as disabled", () => {
    expect(resolveGlobalMaxConcurrentRuns(undefined)).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("")).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("abc")).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("-1")).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("0")).toBe(0);
  });

  it("accepts positive values and floors fractions", () => {
    expect(resolveGlobalMaxConcurrentRuns("2")).toBe(2);
    expect(resolveGlobalMaxConcurrentRuns("2.9")).toBe(2);
    expect(resolveGlobalMaxConcurrentRuns(4)).toBe(4);
  });

  it("uses the environment when no override is supplied", () => {
    const previous = process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS;
    process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS = "3";
    try {
      expect(resolveGlobalMaxConcurrentRuns()).toBe(3);
    } finally {
      if (previous === undefined) {
        delete process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS;
      } else {
        process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS = previous;
      }
    }
  });
});

describeDatabase("global heartbeat run admission", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null =
    null;
  let heartbeat!: ReturnType<typeof heartbeatService>;

  beforeAll(async () => {
    if (externalDatabaseUrl) {
      db = createDb(externalDatabaseUrl);
      heartbeat = heartbeatService(db, { globalMaxConcurrentRuns: 2 });
      return;
    }
    tempDb = await startEmbeddedPostgresTestDatabase(
      "heartbeat-global-run-admission-",
    );
    db = createDb(tempDb.connectionString);
    heartbeat = heartbeatService(db, { globalMaxConcurrentRuns: 2 });
  }, 20_000);

  afterEach(async () => {
    await heartbeat.drainActiveRunExecutions();
    mockAdapterExecute.mockClear();
    runningProcesses.clear();
    await db.execute(sql`truncate table ${companies} cascade`);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function insertCompanyAndAgent(name: string) {
    const companyId = randomUUID();
    const agentId = randomUUID();

    await db.insert(companies).values({
      id: companyId,
      name: `Global Admission ${name}`,
      status: "active",
      issuePrefix: `G${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });

    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: `Global Admission Agent ${name}`,
      role: "engineer",
      status: "idle",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: {
        heartbeat: {
          enabled: true,
          intervalSec: 60,
          wakeOnDemand: true,
          maxConcurrentRuns: 1,
        },
      },
      permissions: {},
    });

    return { companyId, agentId };
  }

  async function insertRunningRun(companyId: string, agentId: string) {
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      invocationSource: "on_demand",
      triggerDetail: "manual",
      status: "running",
      startedAt: new Date(),
      contextSnapshot: {},
    });
    return runId;
  }

  async function insertClaimableQueuedRun(
    companyId: string,
    agentId: string,
    triggerDetail: "manual" | "system" = "manual",
  ) {
    const wakeupRequestId = randomUUID();
    const runId = randomUUID();

    await db.insert(agentWakeupRequests).values({
      id: wakeupRequestId,
      companyId,
      agentId,
      source: "on_demand",
      triggerDetail,
      status: "queued",
      runId,
      requestedByActorType: "user",
      requestedByActorId: "board-user",
      payload: { manualUserWake: true },
    });
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      invocationSource: "on_demand",
      triggerDetail,
      status: "queued",
      wakeupRequestId,
      contextSnapshot: {},
    });

    return { runId, wakeupRequestId };
  }

  async function runStatus(runId: string) {
    return db
      .select({ status: heartbeatRuns.status })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, runId))
      .then((rows) => rows[0] ?? null);
  }

  it("admits only when the running count is below the limit", async () => {
    const { companyId, agentId } = await insertCompanyAndAgent("limit");
    await insertRunningRun(companyId, agentId);
    await insertRunningRun(companyId, agentId);

    await db.transaction(async (tx) => {
      expect(await admitGlobalRunSlot(tx as unknown as Db, 2)).toBe(false);
      expect(await admitGlobalRunSlot(tx as unknown as Db, 3)).toBe(true);
      expect(await admitGlobalRunSlot(tx as unknown as Db, 0)).toBe(true);
    });
  }, 90_000);

  it("leaves a queued run queued while the limit is full and starts it after a slot frees", async () => {
    const first = await insertCompanyAndAgent("running");
    const second = await insertCompanyAndAgent("queued");
    const firstRunA = await insertRunningRun(first.companyId, first.agentId);
    await insertRunningRun(first.companyId, first.agentId);
    const { runId: queuedRunId } = await insertClaimableQueuedRun(
      second.companyId,
      second.agentId,
    );

    await heartbeat.resumeQueuedRuns();

    expect(await runStatus(queuedRunId)).toMatchObject({ status: "queued" });
    expect(mockAdapterExecute).not.toHaveBeenCalled();

    await db
      .update(heartbeatRuns)
      .set({ status: "succeeded", finishedAt: new Date() })
      .where(eq(heartbeatRuns.id, firstRunA));

    await heartbeat.resumeQueuedRuns();
    await heartbeat.drainActiveRunExecutions();

    expect(await runStatus(queuedRunId)).toMatchObject({ status: "succeeded" });
    expect(mockAdapterExecute).toHaveBeenCalledOnce();
  }, 90_000);

  it("prioritizes manual CoS runs over older background work when a slot frees", async () => {
    const first = await insertCompanyAndAgent("priority-running");
    const worker = await insertCompanyAndAgent("priority-worker");
    const cos = await insertCompanyAndAgent("priority-cos");
    const firstRun = await insertRunningRun(first.companyId, first.agentId);
    await insertRunningRun(first.companyId, first.agentId);
    const { runId: workerRunId } = await insertClaimableQueuedRun(
      worker.companyId,
      worker.agentId,
      "system",
    );
    const { runId: cosRunId } = await insertClaimableQueuedRun(
      cos.companyId,
      cos.agentId,
      "manual",
    );

    await db
      .update(heartbeatRuns)
      .set({ status: "succeeded", finishedAt: new Date() })
      .where(eq(heartbeatRuns.id, firstRun));

    let releaseAdapter!: () => void;
    const adapterGate = new Promise<void>((resolve) => {
      releaseAdapter = resolve;
    });
    mockAdapterExecute.mockImplementationOnce(async () => {
      await adapterGate;
      return {
        exitCode: 0,
        signal: null,
        timedOut: false,
        errorMessage: null,
        summary: "Prioritized CoS run.",
        provider: "test",
        model: "test-model",
      };
    });

    await heartbeat.resumeNextGlobalRun();

    expect(await runStatus(cosRunId)).toMatchObject({ status: "running" });
    expect(await runStatus(workerRunId)).toMatchObject({ status: "queued" });

    releaseAdapter();
    await heartbeat.drainActiveRunExecutions();
    await heartbeat.resumeNextGlobalRun();
    await heartbeat.drainActiveRunExecutions();

    expect(await runStatus(cosRunId)).toMatchObject({ status: "succeeded" });
    expect(await runStatus(workerRunId)).toMatchObject({ status: "succeeded" });
    expect(mockAdapterExecute).toHaveBeenCalledTimes(2);
  }, 90_000);

  it("prioritizes a manual run over an older background run for the same agent", async () => {
    const running = await insertCompanyAndAgent("same-agent-running");
    const agent = await insertCompanyAndAgent("same-agent");
    await insertRunningRun(running.companyId, running.agentId);
    const { runId: backgroundRunId } = await insertClaimableQueuedRun(
      agent.companyId,
      agent.agentId,
      "system",
    );
    const { runId: manualRunId } = await insertClaimableQueuedRun(
      agent.companyId,
      agent.agentId,
      "manual",
    );

    let releaseAdapter!: () => void;
    const adapterGate = new Promise<void>((resolve) => {
      releaseAdapter = resolve;
    });
    mockAdapterExecute.mockImplementationOnce(async () => {
      await adapterGate;
      return {
        exitCode: 0,
        signal: null,
        timedOut: false,
        errorMessage: null,
        summary: "Prioritized same-agent run.",
        provider: "test",
        model: "test-model",
      };
    });

    await heartbeat.resumeNextGlobalRun();

    expect(await runStatus(manualRunId)).toMatchObject({ status: "running" });
    expect(await runStatus(backgroundRunId)).toMatchObject({ status: "queued" });

    releaseAdapter();
    await heartbeat.drainActiveRunExecutions();
    await heartbeat.resumeNextGlobalRun();
    await heartbeat.drainActiveRunExecutions();

    expect(await runStatus(manualRunId)).toMatchObject({ status: "succeeded" });
    expect(await runStatus(backgroundRunId)).toMatchObject({ status: "succeeded" });
    expect(mockAdapterExecute).toHaveBeenCalledTimes(2);
  }, 90_000);

  it("does not limit when the configured limit is zero", async () => {
    const first = await insertCompanyAndAgent("unlimited-running");
    const second = await insertCompanyAndAgent("unlimited-queued");
    await insertRunningRun(first.companyId, first.agentId);
    await insertRunningRun(first.companyId, first.agentId);
    const { runId: queuedRunId } = await insertClaimableQueuedRun(
      second.companyId,
      second.agentId,
    );

    const unlimited = heartbeatService(db, { globalMaxConcurrentRuns: 0 });
    await unlimited.resumeQueuedRuns();
    await unlimited.drainActiveRunExecutions();

    expect(await runStatus(queuedRunId)).toMatchObject({ status: "succeeded" });
    expect(mockAdapterExecute).toHaveBeenCalledOnce();
  }, 90_000);
});
