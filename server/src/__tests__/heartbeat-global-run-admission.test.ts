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
  globalQueueCandidateQuery,
  listGlobalQueuedRunCandidates,
  readGlobalQueueBacklogStats,
  resolveGlobalMaxConcurrentRuns,
  resolveGlobalMaxConcurrentRunsPerCompany,
} from "../services/heartbeat/global-run-admission.ts";
import { logger } from "../middleware/logger.ts";
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
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("treats unset, blank, and explicit zero values as disabled", () => {
    expect(resolveGlobalMaxConcurrentRuns(undefined)).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns(null)).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("")).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("   ")).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns("0")).toBe(0);
    expect(resolveGlobalMaxConcurrentRuns(0)).toBe(0);
  });

  it("accepts positive values and floors fractions", () => {
    expect(resolveGlobalMaxConcurrentRuns("2")).toBe(2);
    expect(resolveGlobalMaxConcurrentRuns("2.9")).toBe(2);
    expect(resolveGlobalMaxConcurrentRuns(4)).toBe(4);
  });

  it.each(["abc", "-1", "Infinity", "1e309"])(
    "warns and throws for the non-empty invalid value %s",
    (value) => {
      const warn = vi.spyOn(logger, "warn").mockImplementation(() => undefined as never);
      expect(() => resolveGlobalMaxConcurrentRuns(value)).toThrow(
        /PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS/,
      );
      expect(warn).toHaveBeenCalledWith(
        expect.objectContaining({ value }),
        expect.any(String),
      );
    },
  );

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

describe("resolveGlobalMaxConcurrentRunsPerCompany", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("uses the same disabled and floor semantics as the global limit", () => {
    expect(resolveGlobalMaxConcurrentRunsPerCompany(undefined)).toBe(0);
    expect(resolveGlobalMaxConcurrentRunsPerCompany(" ")).toBe(0);
    expect(resolveGlobalMaxConcurrentRunsPerCompany("0")).toBe(0);
    expect(resolveGlobalMaxConcurrentRunsPerCompany("2.9")).toBe(2);
  });

  it("warns and throws for invalid values", () => {
    const warn = vi.spyOn(logger, "warn").mockImplementation(() => undefined as never);
    expect(() => resolveGlobalMaxConcurrentRunsPerCompany("abc")).toThrow(
      /PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY/,
    );
    expect(warn).toHaveBeenCalled();
  });
});

describeDatabase("global heartbeat run admission", () => {
  let db!: ReturnType<typeof createDb>;
  let connectionString!: string;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null =
    null;
  let heartbeat!: ReturnType<typeof heartbeatService>;

  beforeAll(async () => {
    if (externalDatabaseUrl) {
      connectionString = externalDatabaseUrl;
    } else {
      tempDb = await startEmbeddedPostgresTestDatabase(
        "heartbeat-global-run-admission-",
      );
      connectionString = tempDb.connectionString;
    }
    db = createDb(connectionString);
    heartbeat = heartbeatService(db, { globalMaxConcurrentRuns: 2 });
  }, 20_000);

  afterEach(async () => {
    await heartbeat.drainActiveRunExecutions();
    vi.restoreAllMocks();
    mockAdapterExecute.mockClear();
    runningProcesses.clear();
    await db.execute(sql`truncate table ${companies} cascade`);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function insertCompanyAndAgent(
    name: string,
    maxConcurrentRuns = 1,
  ) {
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
          maxConcurrentRuns,
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

  async function insertQueuedRun(
    companyId: string,
    agentId: string,
    options: { triggerDetail?: "manual" | "system"; createdAt?: Date } = {},
  ) {
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      invocationSource: "on_demand",
      triggerDetail: options.triggerDetail ?? "manual",
      status: "queued",
      createdAt: options.createdAt,
      contextSnapshot: {},
    });
    return runId;
  }

  async function backdateQueuedRun(runId: string, createdAt: Date) {
    await db
      .update(heartbeatRuns)
      .set({ createdAt })
      .where(eq(heartbeatRuns.id, runId));
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

  it("throws during service construction when the configured cap cannot be parsed", () => {
    const previousRuns = process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS;
    const previousCompanies =
      process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY;
    vi.spyOn(logger, "warn").mockImplementation(() => undefined as never);
    process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS = "not-a-number";
    delete process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY;
    try {
      expect(() => heartbeatService(db)).toThrow(
        /PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS/,
      );
    } finally {
      if (previousRuns === undefined) {
        delete process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS;
      } else {
        process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS = previousRuns;
      }
      if (previousCompanies === undefined) {
        delete process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY;
      } else {
        process.env.PAPERCLIP_GLOBAL_MAX_CONCURRENT_RUNS_PER_COMPANY =
          previousCompanies;
      }
    }
  });

  it("lets exactly one of two concurrent claims win the last global slot", async () => {
    const running = await insertCompanyAndAgent("contention-running");
    const left = await insertCompanyAndAgent("contention-left");
    const right = await insertCompanyAndAgent("contention-right");
    await insertRunningRun(running.companyId, running.agentId);
    const { runId: leftRunId } = await insertClaimableQueuedRun(
      left.companyId,
      left.agentId,
    );
    const { runId: rightRunId } = await insertClaimableQueuedRun(
      right.companyId,
      right.agentId,
    );

    const leftDb = createDb(connectionString);
    const rightDb = createDb(connectionString);

    let arrived = 0;
    let releaseBarrier!: () => void;
    let allArrived!: () => void;
    const barrierReleased = new Promise<void>((resolve) => {
      releaseBarrier = resolve;
    });
    const bothArrived = new Promise<void>((resolve) => {
      allArrived = resolve;
    });
    const waitAtBarrier = async () => {
      arrived += 1;
      if (arrived === 2) allArrived();
      await barrierReleased;
    };
    const claimInOwnTransaction = async (client: typeof db, runId: string) =>
      client.transaction(async (tx) => {
        await waitAtBarrier();
        const admitted = await admitGlobalRunSlot(tx as unknown as Db, 2);
        if (admitted) {
          await tx
            .update(heartbeatRuns)
            .set({ status: "running", startedAt: new Date() })
            .where(eq(heartbeatRuns.id, runId));
        }
        return admitted;
      });

    try {
      const leftClaim = claimInOwnTransaction(leftDb, leftRunId);
      const rightClaim = claimInOwnTransaction(rightDb, rightRunId);
      await bothArrived;
      releaseBarrier();
      const [leftAdmitted, rightAdmitted] = await Promise.all([
        leftClaim,
        rightClaim,
      ]);

      expect([leftAdmitted, rightAdmitted].filter(Boolean)).toHaveLength(1);

      const winnerRunId = leftAdmitted ? leftRunId : rightRunId;
      const loserRunId = leftAdmitted ? rightRunId : leftRunId;
      expect(await runStatus(winnerRunId)).toMatchObject({ status: "running" });
      expect(await runStatus(loserRunId)).toMatchObject({ status: "queued" });

      await db
        .update(heartbeatRuns)
        .set({ status: "succeeded", finishedAt: new Date() })
        .where(eq(heartbeatRuns.id, winnerRunId));

      await heartbeat.resumeQueuedRuns();
      await heartbeat.drainActiveRunExecutions();

      expect(await runStatus(loserRunId)).toMatchObject({ status: "succeeded" });
      expect(mockAdapterExecute).toHaveBeenCalledOnce();
    } finally {
      await leftDb.$client.end({ timeout: 5 });
      await rightDb.$client.end({ timeout: 5 });
    }
  }, 90_000);

  it("claims at most one queued run per agent in a single global pass", async () => {
    const worker = await insertCompanyAndAgent("one-claim", 2);
    const { runId: firstRunId } = await insertClaimableQueuedRun(
      worker.companyId,
      worker.agentId,
    );
    const { runId: secondRunId } = await insertClaimableQueuedRun(
      worker.companyId,
      worker.agentId,
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
        summary: "One claim per agent.",
        provider: "test",
        model: "test-model",
      };
    });

    await heartbeat.resumeNextGlobalRun();

    const firstStatus = (await runStatus(firstRunId))?.status;
    const secondStatus = (await runStatus(secondRunId))?.status;
    expect([firstStatus, secondStatus].sort()).toEqual(["queued", "running"]);

    releaseAdapter();
    await heartbeat.drainActiveRunExecutions();
    await heartbeat.resumeNextGlobalRun();
    await heartbeat.drainActiveRunExecutions();

    expect(await runStatus(firstRunId)).toMatchObject({ status: "succeeded" });
    expect(await runStatus(secondRunId)).toMatchObject({ status: "succeeded" });
    expect(mockAdapterExecute).toHaveBeenCalledTimes(2);
  }, 90_000);

  it("starts aged background work ahead of newer manual work and logs backlog age", async () => {
    const background = await insertCompanyAndAgent("aged-background");
    const manualA = await insertCompanyAndAgent("manual-a");
    const manualB = await insertCompanyAndAgent("manual-b");
    const manualC = await insertCompanyAndAgent("manual-c");
    const { runId: backgroundRunId } = await insertClaimableQueuedRun(
      background.companyId,
      background.agentId,
      "system",
    );
    await backdateQueuedRun(backgroundRunId, new Date(Date.now() - 20 * 60_000));
    await insertClaimableQueuedRun(manualA.companyId, manualA.agentId, "manual");
    await insertClaimableQueuedRun(manualB.companyId, manualB.agentId, "manual");
    const { runId: manualCRunId } = await insertClaimableQueuedRun(
      manualC.companyId,
      manualC.agentId,
      "manual",
    );

    const info = vi.spyOn(logger, "info");

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
        summary: "Aged background run.",
        provider: "test",
        model: "test-model",
      };
    });

    await heartbeat.resumeNextGlobalRun();

    expect(await runStatus(backgroundRunId)).toMatchObject({ status: "running" });
    expect(await runStatus(manualCRunId)).toMatchObject({ status: "queued" });

    const backlogLog = info.mock.calls.find(
      ([, message]) => message === "global run queue backlog",
    );
    expect(backlogLog).toBeDefined();
    expect(backlogLog?.[0]).toMatchObject({ queuedCount: 4 });
    expect(
      (backlogLog?.[0] as { oldestQueuedAgeMs?: number }).oldestQueuedAgeMs,
    ).toBeGreaterThanOrEqual(15 * 60_000);

    releaseAdapter();
    await heartbeat.drainActiveRunExecutions();
  }, 90_000);

  it("enforces the per-company share before admitting another run", async () => {
    const companyA = await insertCompanyAndAgent("share-a", 2);
    const companyB = await insertCompanyAndAgent("share-b", 1);
    await insertRunningRun(companyA.companyId, companyA.agentId);
    const { runId: companyAQueuedRunId } = await insertClaimableQueuedRun(
      companyA.companyId,
      companyA.agentId,
    );
    const { runId: companyBQueuedRunId } = await insertClaimableQueuedRun(
      companyB.companyId,
      companyB.agentId,
    );

    const shared = heartbeatService(db, {
      globalMaxConcurrentRuns: 10,
      globalMaxConcurrentRunsPerCompany: 1,
    });

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
        summary: "Per-company share run.",
        provider: "test",
        model: "test-model",
      };
    });

    await shared.resumeNextGlobalRun();

    expect(await runStatus(companyAQueuedRunId)).toMatchObject({ status: "queued" });
    expect(await runStatus(companyBQueuedRunId)).toMatchObject({ status: "running" });

    releaseAdapter();
    await shared.drainActiveRunExecutions();
  }, 90_000);

  it("reports backlog stats for the oldest queued run", async () => {
    const worker = await insertCompanyAndAgent("backlog");
    const oldest = new Date(Date.now() - 10 * 60_000);
    await insertQueuedRun(worker.companyId, worker.agentId, {
      createdAt: oldest,
    });
    await insertQueuedRun(worker.companyId, worker.agentId, {
      createdAt: new Date(Date.now() - 60_000),
    });

    const stats = await readGlobalQueueBacklogStats(db, null);

    expect(stats.queuedCount).toBe(2);
    expect(stats.oldestQueuedAt?.getTime()).toBe(oldest.getTime());
    expect(stats.oldestQueuedAgeMs).toBeGreaterThanOrEqual(9 * 60_000);
  }, 90_000);

  it("bounds candidate selection to the requested scan limit", async () => {
    const companyId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Candidate limit company",
      status: "active",
      issuePrefix: `GC${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });
    const agentIds = Array.from({ length: 50 }, () => randomUUID());
    await db.insert(agents).values(
      agentIds.map((agentId, index) => ({
        id: agentId,
        companyId,
        name: `Candidate agent ${index}`,
        role: "engineer",
        status: "idle",
        adapterType: "codex_local",
        adapterConfig: {},
        runtimeConfig: {},
        permissions: {},
      })),
    );
    await db.insert(heartbeatRuns).values(
      agentIds.map((agentId) => ({
        id: randomUUID(),
        companyId,
        agentId,
        invocationSource: "on_demand",
        triggerDetail: "manual",
        status: "queued",
        contextSnapshot: {},
      })),
    );

    const candidates = await listGlobalQueuedRunCandidates(db, {
      cutoff: null,
      limit: 5,
    });

    expect(candidates).toHaveLength(5);
  }, 90_000);

  it("uses the partial index for the bounded candidate query", async () => {
    const companyId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Candidate plan company",
      status: "active",
      issuePrefix: `GP${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });
    const agentIds = Array.from({ length: 50 }, () => randomUUID());
    await db.insert(agents).values(
      agentIds.map((agentId, index) => ({
        id: agentId,
        companyId,
        name: `Plan agent ${index}`,
        role: "engineer",
        status: "idle",
        adapterType: "codex_local",
        adapterConfig: {},
        runtimeConfig: {},
        permissions: {},
      })),
    );
    await db.insert(heartbeatRuns).values(
      agentIds.map((agentId) => ({
        id: randomUUID(),
        companyId,
        agentId,
        invocationSource: "on_demand",
        triggerDetail: "manual",
        status: "queued",
        contextSnapshot: {},
      })),
    );

    await db.transaction(async (tx) => {
      await tx.execute(sql`set local enable_seqscan = off`);
      const planRows = await tx.execute(sql`
        explain (format json) ${globalQueueCandidateQuery(null, 5)}
      `);
      const plan = JSON.stringify(planRows);
      expect(plan).toContain(
        "heartbeat_runs_queued_company_agent_created_idx",
      );
    });
  }, 90_000);
});
