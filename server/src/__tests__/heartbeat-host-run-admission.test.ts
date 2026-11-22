import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents,
  agentWakeupRequests,
  authUsers,
  companies,
  companyMemberships,
  createDb,
  environments,
  heartbeatRuns,
  issues,
  type Db,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.ts";
import {
  admitHostQueuedRun,
  admitHostRunSlot,
  readRunAdmissionHostKey,
  resolveEnvironmentHostKey,
  resolveMaxConcurrentRunsPerHost,
  resolveMaxQueuedRunsPerHost,
  resolveRunAdmissionHostKey,
  withRunAdmissionHostKey,
} from "../services/heartbeat/host-run-admission.ts";
import { HOST_QUEUE_DEPTH_WAKE_REASON } from "../services/heartbeat/queue.ts";
import { runningProcesses } from "../adapters/index.ts";

const mockAdapterExecute = vi.hoisted(() =>
  vi.fn(async () => ({
    exitCode: 0,
    signal: null,
    timedOut: false,
    errorMessage: null,
    summary: "Host run admission test run.",
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
    `Skipping embedded Postgres host run admission tests on this host: ${
      embeddedPostgresSupport.reason ?? "unsupported environment"
    }`,
  );
}

describe("resolveMaxConcurrentRunsPerHost", () => {
  it("treats unset, invalid, and non-positive values as disabled", () => {
    expect(resolveMaxConcurrentRunsPerHost(undefined)).toBe(0);
    expect(resolveMaxConcurrentRunsPerHost("")).toBe(0);
    expect(resolveMaxConcurrentRunsPerHost("abc")).toBe(0);
    expect(resolveMaxConcurrentRunsPerHost("-1")).toBe(0);
    expect(resolveMaxConcurrentRunsPerHost("0")).toBe(0);
  });

  it("accepts positive values and floors fractions", () => {
    expect(resolveMaxConcurrentRunsPerHost("3")).toBe(3);
    expect(resolveMaxConcurrentRunsPerHost("3.9")).toBe(3);
    expect(resolveMaxConcurrentRunsPerHost(2)).toBe(2);
  });

  it("uses the environment when no override is supplied", () => {
    const previous = process.env.PAPERCLIP_MAX_CONCURRENT_RUNS_PER_HOST;
    process.env.PAPERCLIP_MAX_CONCURRENT_RUNS_PER_HOST = "3";
    try {
      expect(resolveMaxConcurrentRunsPerHost()).toBe(3);
    } finally {
      if (previous === undefined) {
        delete process.env.PAPERCLIP_MAX_CONCURRENT_RUNS_PER_HOST;
      } else {
        process.env.PAPERCLIP_MAX_CONCURRENT_RUNS_PER_HOST = previous;
      }
    }
  });
});

describe("resolveMaxQueuedRunsPerHost", () => {
  it("treats unset, invalid, and non-positive values as disabled", () => {
    expect(resolveMaxQueuedRunsPerHost(undefined)).toBe(0);
    expect(resolveMaxQueuedRunsPerHost("")).toBe(0);
    expect(resolveMaxQueuedRunsPerHost("abc")).toBe(0);
    expect(resolveMaxQueuedRunsPerHost("-1")).toBe(0);
    expect(resolveMaxQueuedRunsPerHost("0")).toBe(0);
  });

  it("accepts positive values and floors fractions", () => {
    expect(resolveMaxQueuedRunsPerHost("1")).toBe(1);
    expect(resolveMaxQueuedRunsPerHost("3.9")).toBe(3);
    expect(resolveMaxQueuedRunsPerHost(2)).toBe(2);
  });

  it("uses the environment when no override is supplied", () => {
    const previous = process.env.PAPERCLIP_MAX_QUEUED_RUNS_PER_HOST;
    process.env.PAPERCLIP_MAX_QUEUED_RUNS_PER_HOST = "4";
    try {
      expect(resolveMaxQueuedRunsPerHost()).toBe(4);
    } finally {
      if (previous === undefined) {
        delete process.env.PAPERCLIP_MAX_QUEUED_RUNS_PER_HOST;
      } else {
        process.env.PAPERCLIP_MAX_QUEUED_RUNS_PER_HOST = previous;
      }
    }
  });
});

describe("resolveEnvironmentHostKey", () => {
  it("groups SSH environments by configured host", () => {
    expect(
      resolveEnvironmentHostKey({
        id: "env-1",
        driver: "ssh",
        config: { host: "10.0.0.43" },
      }),
    ).toBe("ssh:10.0.0.43");
    expect(
      resolveEnvironmentHostKey({
        id: "env-2",
        driver: "ssh",
        config: { host: " 10.0.0.43 " },
      }),
    ).toBe("ssh:10.0.0.43");
  });

  it("keeps local and other drivers distinct", () => {
    expect(
      resolveEnvironmentHostKey({ id: "local", driver: "local", config: {} }),
    ).toBe("local");
    expect(
      resolveEnvironmentHostKey({ id: "sandbox-1", driver: "sandbox", config: {} }),
    ).toBe("env:sandbox-1");
  });

  it("falls back to the environment id when SSH has no host", () => {
    expect(
      resolveEnvironmentHostKey({ id: "ssh-1", driver: "ssh", config: {} }),
    ).toBe("env:ssh-1");
  });
});

describe("run admission host key", () => {
  it("round-trips the host key in the run context snapshot", () => {
    const context = withRunAdmissionHostKey({ issueId: "issue-1" }, "ssh:10.0.0.41");
    expect(readRunAdmissionHostKey(context)).toBe("ssh:10.0.0.41");
    expect(context).toMatchObject({ issueId: "issue-1" });
  });

  it("ignores empty host keys", () => {
    expect(readRunAdmissionHostKey({})).toBeNull();
    expect(readRunAdmissionHostKey({ paperclipAdmissionHostKey: "  " })).toBeNull();
  });
});

describeDatabase("host heartbeat run admission", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null =
    null;
  let heartbeat!: ReturnType<typeof heartbeatService>;

  beforeAll(async () => {
    if (externalDatabaseUrl) {
      db = createDb(externalDatabaseUrl);
      heartbeat = heartbeatService(db, { maxConcurrentRunsPerHost: 3 });
      return;
    }
    tempDb = await startEmbeddedPostgresTestDatabase(
      "heartbeat-host-run-admission-",
    );
    db = createDb(tempDb.connectionString);
    heartbeat = heartbeatService(db, { maxConcurrentRunsPerHost: 3 });
  }, 20_000);

  afterEach(async () => {
    await heartbeat.drainActiveRunExecutions();
    mockAdapterExecute.mockClear();
    runningProcesses.clear();
    await db.execute(sql`truncate table ${companies} cascade`);
    await db.delete(environments);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function insertEnvironment(host: string) {
    const environmentId = randomUUID();
    await db.insert(environments).values({
      id: environmentId,
      name: `host-admission-${environmentId}`,
      driver: "ssh",
      status: "active",
      config: { host },
      envVars: {},
    });
    return environmentId;
  }

  async function insertLocalEnvironment() {
    const environmentId = randomUUID();
    await db.insert(environments).values({
      id: environmentId,
      name: `host-admission-${environmentId}`,
      driver: "local",
      status: "active",
      config: {},
      envVars: {},
    });
    return environmentId;
  }

  async function insertCompanyAndAgent(
    name: string,
    environmentId: string | null = null,
  ) {
    const companyId = randomUUID();
    const agentId = randomUUID();

    await db.insert(companies).values({
      id: companyId,
      name: `Host Admission ${name}`,
      status: "active",
      issuePrefix: `H${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      defaultResponsibleUserId: "responsible-user",
      requireBoardApprovalForNewAgents: false,
    });
    // Wake admission intersects agent authority with an existing responsible
    // user and that user's active company membership.
    await db.insert(authUsers).values({
      id: "responsible-user",
      name: "Fixture user",
      email: "responsible-user@example.test",
      createdAt: new Date(),
      updatedAt: new Date(),
    }).onConflictDoNothing();
    await db.insert(companyMemberships).values({
      companyId,
      principalType: "user",
      principalId: "responsible-user",
      status: "active",
      membershipRole: "member",
    });

    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: `Host Admission Agent ${name}`,
      role: "engineer",
      status: "idle",
      adapterType: "codex_local",
      adapterConfig: {},
      defaultEnvironmentId: environmentId,
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

  async function insertIssue(companyId: string, agentId: string) {
    const [issue] = await db
      .insert(issues)
      .values({
        companyId,
        title: `Host queue depth issue ${randomUUID()}`,
        status: "todo",
        assigneeAgentId: agentId,
      })
      .returning();
    return issue.id;
  }

  async function insertRunningRun(
    companyId: string,
    agentId: string,
    hostKey: string,
  ) {
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      invocationSource: "on_demand",
      triggerDetail: "manual",
      status: "running",
      startedAt: new Date(),
      contextSnapshot: withRunAdmissionHostKey({}, hostKey),
    });
    return runId;
  }

  async function insertClaimableQueuedRun(
    companyId: string,
    agentId: string,
    hostKey: string | null = null,
  ) {
    const wakeupRequestId = randomUUID();
    const runId = randomUUID();

    await db.insert(agentWakeupRequests).values({
      id: wakeupRequestId,
      companyId,
      agentId,
      source: "on_demand",
      triggerDetail: "manual",
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
      triggerDetail: "manual",
      status: "queued",
      wakeupRequestId,
      contextSnapshot: hostKey ? withRunAdmissionHostKey({}, hostKey) : {},
    });

    return { runId, wakeupRequestId };
  }

  async function insertHostQueuedRun(
    companyId: string,
    agentId: string,
    hostKey: string | null,
    status: "queued" | "scheduled_retry",
  ) {
    const runId = randomUUID();
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      invocationSource: "on_demand",
      triggerDetail: "manual",
      status,
      contextSnapshot: hostKey ? withRunAdmissionHostKey({}, hostKey) : {},
    });
    return runId;
  }

  async function hostQueueDepthWakes(agentId: string) {
    return db
      .select()
      .from(agentWakeupRequests)
      .where(
        and(
          eq(agentWakeupRequests.agentId, agentId),
          eq(agentWakeupRequests.reason, HOST_QUEUE_DEPTH_WAKE_REASON),
        ),
      );
  }

  async function backdateHostQueueDepthWakes() {
    await db
      .update(agentWakeupRequests)
      .set({ updatedAt: new Date(Date.now() - 60_000) })
      .where(eq(agentWakeupRequests.reason, HOST_QUEUE_DEPTH_WAKE_REASON));
  }

  async function countQueuedRuns() {
    const [row] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.status, "queued"));
    return Number(row?.count ?? 0);
  }

  async function runStatus(runId: string) {
    return db
      .select({ status: heartbeatRuns.status })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, runId))
      .then((rows) => rows[0] ?? null);
  }

  it("resolves the same host key for two environments on one machine", async () => {
    const first = await insertEnvironment("10.0.0.43");
    const second = await insertEnvironment("10.0.0.43");

    expect(await resolveRunAdmissionHostKey(db, { environmentId: first })).toBe(
      "ssh:10.0.0.43",
    );
    expect(await resolveRunAdmissionHostKey(db, { environmentId: second })).toBe(
      "ssh:10.0.0.43",
    );
  });

  it("admits only when the running count on the host is below the limit", async () => {
    const environmentId = await insertEnvironment("10.0.0.41");
    const { companyId, agentId } = await insertCompanyAndAgent(
      "direct",
      environmentId,
    );
    await insertRunningRun(companyId, agentId, "ssh:10.0.0.41");
    await insertRunningRun(companyId, agentId, "ssh:10.0.0.41");
    await insertRunningRun(companyId, agentId, "ssh:10.0.0.41");
    await insertRunningRun(companyId, agentId, "ssh:10.0.0.42");

    await db.transaction(async (tx) => {
      expect(
        await admitHostRunSlot(tx as unknown as Db, 3, "ssh:10.0.0.41"),
      ).toBe(false);
      expect(
        await admitHostRunSlot(tx as unknown as Db, 4, "ssh:10.0.0.41"),
      ).toBe(true);
      expect(
        await admitHostRunSlot(tx as unknown as Db, 3, "ssh:10.0.0.42"),
      ).toBe(true);
      expect(await admitHostRunSlot(tx as unknown as Db, 0, "ssh:10.0.0.41")).toBe(
        true,
      );
      expect(await admitHostRunSlot(tx as unknown as Db, 3, null)).toBe(true);
    });
  }, 90_000);

  it("leaves a queued run queued while the host is full and starts it after a slot frees", async () => {
    const environmentId = await insertLocalEnvironment();
    const first = await insertCompanyAndAgent("running", environmentId);
    const second = await insertCompanyAndAgent("queued", environmentId);
    const firstRunA = await insertRunningRun(
      first.companyId,
      first.agentId,
      "local",
    );
    await insertRunningRun(
      first.companyId,
      first.agentId,
      "local",
    );
    await insertRunningRun(
      first.companyId,
      first.agentId,
      "local",
    );
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

  it("shares the cap across two environments on the same host", async () => {
    const firstEnvironment = await insertEnvironment("10.0.0.43");
    const secondEnvironment = await insertEnvironment("10.0.0.43");
    const first = await insertCompanyAndAgent("host-a-running", firstEnvironment);
    const second = await insertCompanyAndAgent("host-a-queued", secondEnvironment);
    await insertRunningRun(first.companyId, first.agentId, "ssh:10.0.0.43");
    await insertRunningRun(first.companyId, first.agentId, "ssh:10.0.0.43");
    await insertRunningRun(first.companyId, first.agentId, "ssh:10.0.0.43");
    const { runId: queuedRunId } = await insertClaimableQueuedRun(
      second.companyId,
      second.agentId,
    );

    await heartbeat.resumeQueuedRuns();

    expect(await runStatus(queuedRunId)).toMatchObject({ status: "queued" });
    expect(mockAdapterExecute).not.toHaveBeenCalled();
  }, 90_000);

  it("admits queued depth below the cap and shares one SSH host across environments", async () => {
    const firstEnvironment = await insertEnvironment("10.0.0.43");
    const secondEnvironment = await insertEnvironment("10.0.0.43");
    const firstKey = await resolveRunAdmissionHostKey(db, {
      environmentId: firstEnvironment,
    });
    const secondKey = await resolveRunAdmissionHostKey(db, {
      environmentId: secondEnvironment,
    });
    expect(firstKey).toBe("ssh:10.0.0.43");
    expect(secondKey).toBe(firstKey);

    const { companyId, agentId } = await insertCompanyAndAgent(
      "queued-depth",
      firstEnvironment,
    );
    await insertHostQueuedRun(companyId, agentId, firstKey, "queued");
    // A different host must not consume this host's queued budget.
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.44", "queued");

    await db.transaction(async (tx) => {
      // queued == maxQueued: refused, and the second environment on the same
      // machine shares the count through the resolved host key.
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 1,
          hostKey: secondKey,
        }),
      ).toBe(false);
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 2,
          hostKey: secondKey,
        }),
      ).toBe(true);
    });

    // A slot opens once the queued run is no longer queued.
    const [queuedRow] = await db
      .select({ id: heartbeatRuns.id })
      .from(heartbeatRuns)
      .where(
        and(
          eq(heartbeatRuns.companyId, companyId),
          eq(heartbeatRuns.status, "queued"),
          sql`${heartbeatRuns.contextSnapshot} ->> 'paperclipAdmissionHostKey' = ${firstKey}`,
        ),
      );
    await db
      .update(heartbeatRuns)
      .set({ status: "running", startedAt: new Date() })
      .where(eq(heartbeatRuns.id, queuedRow.id));
    await db.transaction(async (tx) => {
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 1,
          hostKey: secondKey,
        }),
      ).toBe(true);
    });
  }, 90_000);

  it("leaves hosts without an environment unconstrained by the queued cap", async () => {
    expect(await resolveRunAdmissionHostKey(db, { environmentId: null })).toBeNull();
    const { companyId, agentId } = await insertCompanyAndAgent("no-env", null);
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.41", "queued");
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.41", "queued");

    await db.transaction(async (tx) => {
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 1,
          hostKey: null,
        }),
      ).toBe(true);
      expect(await admitHostRunSlot(tx as unknown as Db, 2, null)).toBe(true);
    });
  }, 90_000);

  it("always admits while the queued-depth cap is disabled", async () => {
    const { companyId, agentId } = await insertCompanyAndAgent("disabled", null);
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.41", "queued");
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.41", "queued");
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.41", "queued");

    await db.transaction(async (tx) => {
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 0,
          hostKey: "ssh:10.0.0.41",
        }),
      ).toBe(true);
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: -1,
          hostKey: "ssh:10.0.0.41",
        }),
      ).toBe(true);
    });
  }, 90_000);

  it("counts scheduled_retry runs toward the queued depth", async () => {
    const { companyId, agentId } = await insertCompanyAndAgent("retry-depth", null);
    await insertHostQueuedRun(companyId, agentId, "ssh:10.0.0.41", "scheduled_retry");

    await db.transaction(async (tx) => {
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 1,
          hostKey: "ssh:10.0.0.41",
        }),
      ).toBe(false);
      expect(
        await admitHostQueuedRun(tx as unknown as Db, {
          maxRunning: 2,
          maxQueued: 2,
          hostKey: "ssh:10.0.0.41",
        }),
      ).toBe(true);
    });
  }, 90_000);

  it("persists the admission host key on queued runs at enqueue", async () => {
    const environmentId = await insertEnvironment("10.0.0.41");
    const running = await insertCompanyAndAgent("key-running", environmentId);
    const incoming = await insertCompanyAndAgent("key-incoming", environmentId);
    await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    const issueId = await insertIssue(incoming.companyId, incoming.agentId);

    const queuedDepthService = heartbeatService(db, {
      maxConcurrentRunsPerHost: 2,
      maxQueuedRunsPerHost: 1,
    });
    // 2 running + 0 queued leaves the queued budget open.
    const run = await queuedDepthService.wakeup(incoming.agentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: "issue_assigned",
      requestedByActorType: "system",
      payload: { issueId },
    });
    expect(run).not.toBeNull();
    const [row] = await db
      .select()
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, run!.id));
    expect(readRunAdmissionHostKey(row.contextSnapshot)).toBe("ssh:10.0.0.41");
    await queuedDepthService.drainActiveRunExecutions();
  }, 90_000);

  it("parks an assignment wake behind a full host queue instead of queueing a run", async () => {
    const environmentId = await insertEnvironment("10.0.0.41");
    const running = await insertCompanyAndAgent("full-running", environmentId);
    const queued = await insertCompanyAndAgent("full-queued", environmentId);
    const incoming = await insertCompanyAndAgent("full-incoming", environmentId);
    await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    await insertClaimableQueuedRun(
      queued.companyId,
      queued.agentId,
      "ssh:10.0.0.41",
    );
    const issueId = await insertIssue(incoming.companyId, incoming.agentId);

    const queuedDepthService = heartbeatService(db, {
      maxConcurrentRunsPerHost: 2,
      maxQueuedRunsPerHost: 1,
    });
    const queuedRunsBefore = await countQueuedRuns();
    const run = await queuedDepthService.wakeup(incoming.agentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: "issue_assigned",
      requestedByActorType: "system",
      payload: { issueId },
    });
    expect(run).toBeNull();
    expect(await countQueuedRuns()).toBe(queuedRunsBefore);

    const parked = await hostQueueDepthWakes(incoming.agentId);
    expect(parked).toHaveLength(1);
    expect(parked[0]).toMatchObject({
      status: "deferred_issue_execution",
      runId: null,
    });
    const payload = parked[0].payload as Record<string, unknown>;
    expect(payload.hostQueueDepth).toEqual({ hostKey: "ssh:10.0.0.41" });
    const runSeed = payload.runSeed as Record<string, unknown>;
    expect(runSeed.invocationSource).toBe("assignment");
    expect(
      readRunAdmissionHostKey(
        runSeed.contextSnapshot as Record<string, unknown>,
      ),
    ).toBe("ssh:10.0.0.41");
    await queuedDepthService.drainActiveRunExecutions();
  }, 90_000);

  it("coalesces repeated wakes for the same agent and issue into one parked wake", async () => {
    const environmentId = await insertEnvironment("10.0.0.41");
    const running = await insertCompanyAndAgent("twice-running", environmentId);
    const queued = await insertCompanyAndAgent("twice-queued", environmentId);
    const incoming = await insertCompanyAndAgent("twice-incoming", environmentId);
    await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    await insertClaimableQueuedRun(
      queued.companyId,
      queued.agentId,
      "ssh:10.0.0.41",
    );
    const issueId = await insertIssue(incoming.companyId, incoming.agentId);

    const queuedDepthService = heartbeatService(db, {
      maxConcurrentRunsPerHost: 2,
      maxQueuedRunsPerHost: 1,
    });
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const run = await queuedDepthService.wakeup(incoming.agentId, {
        source: "assignment",
        triggerDetail: "system",
        reason: "issue_assigned",
        requestedByActorType: "system",
        payload: { issueId },
      });
      expect(run).toBeNull();
    }

    const parked = await hostQueueDepthWakes(incoming.agentId);
    expect(parked).toHaveLength(1);
    expect(parked[0].status).toBe("deferred_issue_execution");
    expect(parked[0].coalescedCount).toBe(1);
    expect(
      await db
        .select({ id: heartbeatRuns.id })
        .from(heartbeatRuns)
        .where(eq(heartbeatRuns.agentId, incoming.agentId)),
    ).toHaveLength(0);
    await queuedDepthService.drainActiveRunExecutions();
  }, 90_000);

  it("promotes a parked wake when host budget frees and runs it to success", async () => {
    // A local environment keeps run execution in the mocked adapter; the
    // queued-depth mechanics are identical for any resolved host key.
    const environmentId = await insertLocalEnvironment();
    const running = await insertCompanyAndAgent("promote-running", environmentId);
    const queued = await insertCompanyAndAgent("promote-queued", environmentId);
    const incoming = await insertCompanyAndAgent("promote-incoming", environmentId);
    await insertRunningRun(running.companyId, running.agentId, "local");
    const freedRunId = await insertRunningRun(
      running.companyId,
      running.agentId,
      "local",
    );
    const { runId: seededQueuedRunId } = await insertClaimableQueuedRun(
      queued.companyId,
      queued.agentId,
      "local",
    );
    const issueId = await insertIssue(incoming.companyId, incoming.agentId);

    const queuedDepthService = heartbeatService(db, {
      maxConcurrentRunsPerHost: 2,
      maxQueuedRunsPerHost: 1,
    });
    expect(
      await queuedDepthService.wakeup(incoming.agentId, {
        source: "assignment",
        triggerDetail: "system",
        reason: "issue_assigned",
        requestedByActorType: "system",
        payload: { issueId },
      }),
    ).toBeNull();
    const [parked] = await hostQueueDepthWakes(incoming.agentId);
    expect(parked.status).toBe("deferred_issue_execution");

    // Free a running slot. The pass claims the seeded queued run into it and
    // then promotes the parked wake into exactly one queued run.
    await db
      .update(heartbeatRuns)
      .set({ status: "succeeded", finishedAt: new Date() })
      .where(eq(heartbeatRuns.id, freedRunId));
    await backdateHostQueueDepthWakes();
    await queuedDepthService.resumeQueuedRuns();
    await queuedDepthService.drainActiveRunExecutions();

    const [promotedWake] = await hostQueueDepthWakes(incoming.agentId);
    expect(promotedWake.status).toBe("queued");
    expect(promotedWake.runId).toBeTruthy();
    const incomingRuns = await db
      .select()
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.agentId, incoming.agentId));
    expect(incomingRuns).toHaveLength(1);
    expect(incomingRuns[0].id).toBe(promotedWake.runId);
    expect(readRunAdmissionHostKey(incomingRuns[0].contextSnapshot)).toBe("local");

    await queuedDepthService.resumeQueuedRuns();
    await queuedDepthService.drainActiveRunExecutions();
    expect(await runStatus(promotedWake.runId!)).toMatchObject({
      status: "succeeded",
    });
    expect(mockAdapterExecute).toHaveBeenCalled();
    expect(await runStatus(seededQueuedRunId)).toMatchObject({
      status: "succeeded",
    });
  }, 90_000);

  it("does not start a promoted run while the global cap is reached", async () => {
    const environmentId = await insertEnvironment("10.0.0.41");
    const running = await insertCompanyAndAgent("global-running", environmentId);
    const incoming = await insertCompanyAndAgent("global-incoming", environmentId);
    for (let i = 0; i < 3; i += 1) {
      await insertRunningRun(running.companyId, running.agentId, "ssh:10.0.0.41");
    }
    const issueId = await insertIssue(incoming.companyId, incoming.agentId);

    const queuedDepthService = heartbeatService(db, {
      globalMaxConcurrentRuns: 1,
      maxConcurrentRunsPerHost: 2,
      maxQueuedRunsPerHost: 1,
    });
    // 3 running + 0 queued fills running + queued budget for the host.
    expect(
      await queuedDepthService.wakeup(incoming.agentId, {
        source: "assignment",
        triggerDetail: "system",
        reason: "issue_assigned",
        requestedByActorType: "system",
        payload: { issueId },
      }),
    ).toBeNull();
    const [parked] = await hostQueueDepthWakes(incoming.agentId);
    expect(parked.status).toBe("deferred_issue_execution");

    // Free host budget down to one running run; the global cap (1) stays
    // reached, so promotion must queue the run without starting it.
    const runningRows = await db
      .select({ id: heartbeatRuns.id })
      .from(heartbeatRuns)
      .where(
        and(
          eq(heartbeatRuns.status, "running"),
          eq(heartbeatRuns.agentId, running.agentId),
        ),
      );
    for (const row of runningRows.slice(1)) {
      await db
        .update(heartbeatRuns)
        .set({ status: "succeeded", finishedAt: new Date() })
        .where(eq(heartbeatRuns.id, row.id));
    }
    await backdateHostQueueDepthWakes();
    await queuedDepthService.resumeQueuedRuns();
    await queuedDepthService.drainActiveRunExecutions();

    const [promotedWake] = await hostQueueDepthWakes(incoming.agentId);
    expect(promotedWake.status).toBe("queued");
    const [promotedRun] = await db
      .select()
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, promotedWake.runId!));
    expect(promotedRun.status).toBe("queued");
    expect(mockAdapterExecute).not.toHaveBeenCalled();
    await queuedDepthService.drainActiveRunExecutions();
  }, 90_000);
});
