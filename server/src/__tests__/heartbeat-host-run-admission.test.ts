import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agents,
  agentWakeupRequests,
  companies,
  createDb,
  environments,
  heartbeatRuns,
  type Db,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.ts";
import {
  admitHostRunSlot,
  readRunAdmissionHostKey,
  resolveEnvironmentHostKey,
  resolveMaxConcurrentRunsPerHost,
  resolveRunAdmissionHostKey,
  withRunAdmissionHostKey,
} from "../services/heartbeat/host-run-admission.ts";
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
      requireBoardApprovalForNewAgents: false,
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
});
