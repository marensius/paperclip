import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  activityLog,
  agents,
  approvals,
  authUsers,
  cases,
  companies,
  companyMemberships,
  createDb,
  decisionTargetIssues,
  decisions,
  heartbeatRuns,
  issueApprovals,
  issueInboxArchives,
  issueRecoveryActions,
  issueThreadInteractions,
  issues,
  principalPermissionGrants,
} from "@paperclipai/db";
import { errorHandler } from "../middleware/index.js";
import { issueRoutes } from "../routes/issues.js";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

const DAY_MS = 86_400_000;

describeEmbeddedPostgres("issue retention purge route", () => {
  let db!: ReturnType<typeof createDb>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-issue-retention-purge-");
    db = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    await db.delete(decisionTargetIssues);
    await db.delete(decisions);
    await db.delete(issueThreadInteractions);
    await db.delete(issueRecoveryActions);
    await db.delete(issueApprovals);
    await db.delete(approvals);
    await db.delete(cases);
    await db.delete(activityLog);
    await db.delete(issueInboxArchives);
    await db.delete(issues);
    await db.delete(heartbeatRuns);
    await db.delete(principalPermissionGrants);
    await db.delete(companyMemberships);
    await db.delete(agents);
    await db.delete(companies);
    await db.delete(authUsers);
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  function appFor(actor: Express.Request["actor"]) {
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.actor = actor;
      next();
    });
    app.use("/api", issueRoutes(db, {} as never));
    app.use(errorHandler);
    return app;
  }

  type Seed = Awaited<ReturnType<typeof seedCompany>>;

  async function seedCompany() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const runId = randomUUID();
    const userId = `user-${randomUUID()}`;
    const now = new Date();
    await db.insert(companies).values({
      id: companyId,
      name: `Retention ${companyId}`,
      issuePrefix: `RP${companyId.replaceAll("-", "").slice(0, 6).toUpperCase()}`,
    });
    await db.insert(authUsers).values({
      id: userId,
      name: "Retention Owner",
      email: `${userId}@example.com`,
      emailVerified: true,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(companyMemberships).values([
      {
        companyId,
        principalType: "user",
        principalId: userId,
        status: "active",
        membershipRole: "owner",
      },
      {
        companyId,
        principalType: "agent",
        principalId: agentId,
        status: "active",
        membershipRole: "member",
      },
    ]);
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Retention Agent",
      role: "engineer",
      status: "active",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    });
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "running",
      invocationSource: "assignment",
      responsibleUserId: userId,
      contextSnapshot: {},
    });
    return { companyId, agentId, runId, userId, now };
  }

  async function grant(seed: Seed, key: string) {
    await db.insert(principalPermissionGrants).values([
      {
        companyId: seed.companyId,
        principalType: "agent",
        principalId: seed.agentId,
        permissionKey: key,
      },
      {
        companyId: seed.companyId,
        principalType: "user",
        principalId: seed.userId,
        permissionKey: key,
      },
    ]);
  }

  async function addIssue(
    seed: Seed,
    input: {
      status: "done" | "cancelled" | "todo" | "in_progress";
      ageDays?: number;
      archived?: boolean;
      extra?: Partial<typeof issues.$inferInsert>;
    },
  ) {
    const id = randomUUID();
    const terminalAt = new Date(seed.now.getTime() - (input.ageDays ?? 10) * DAY_MS);
    await db.insert(issues).values({
      id,
      companyId: seed.companyId,
      identifier: `RET-${id.slice(0, 8)}`,
      title: `Retention issue ${id}`,
      status: input.status,
      priority: "medium",
      createdByUserId: seed.userId,
      completedAt: input.status === "done" ? terminalAt : null,
      cancelledAt: input.status === "cancelled" ? terminalAt : null,
      updatedAt: terminalAt,
      ...input.extra,
    });
    if (input.archived !== false) {
      await db.insert(issueInboxArchives).values({
        companyId: seed.companyId,
        issueId: id,
        userId: seed.userId,
        archivedAt: terminalAt,
      });
    }
    return id;
  }

  function boardActor(seed: Seed): Express.Request["actor"] {
    return {
      type: "board",
      source: "local_implicit",
      userId: seed.userId,
      companyIds: [seed.companyId],
      isInstanceAdmin: true,
    };
  }

  function agentActor(seed: Seed): Express.Request["actor"] {
    return {
      type: "agent",
      source: "agent_jwt",
      agentId: seed.agentId,
      companyId: seed.companyId,
      runId: seed.runId,
      onBehalfOfUserId: seed.userId,
      onBehalfOfMemberships: [
        { companyId: seed.companyId, membershipRole: "owner", status: "active" },
      ],
    };
  }

  function purge(seed: Seed, actor: Express.Request["actor"], body: Record<string, unknown> = {}) {
    return request(appFor(actor))
      .post(`/api/companies/${seed.companyId}/issues/retention-purge`)
      .send(body);
  }

  it("deletes only archived terminal issues older than the cutoff and skips every restricted reference", async () => {
    const seed = await seedCompany();
    await grant(seed, "issues:retention_delete");

    const eligibleDone = await addIssue(seed, { status: "done", ageDays: 10 });
    const eligibleCancelled = await addIssue(seed, { status: "cancelled", ageDays: 8 });

    const tooYoung = await addIssue(seed, { status: "done", ageDays: 3 });
    const notArchived = await addIssue(seed, { status: "done", ageDays: 10, archived: false });
    const running = await addIssue(seed, { status: "in_progress", ageDays: 10 });
    const monitored = await addIssue(seed, {
      status: "done",
      ageDays: 10,
      extra: { monitorNextCheckAt: seed.now },
    });
    const checkedOut = await addIssue(seed, {
      status: "done",
      ageDays: 10,
      extra: { checkoutRunId: seed.runId },
    });
    const withOpenChild = await addIssue(seed, { status: "done", ageDays: 10 });
    await addIssue(seed, {
      status: "todo",
      ageDays: 1,
      archived: false,
      extra: { parentId: withOpenChild },
    });
    const withTerminalChild = await addIssue(seed, { status: "done", ageDays: 10 });
    await addIssue(seed, {
      status: "done",
      ageDays: 10,
      archived: false,
      extra: { parentId: withTerminalChild },
    });
    const withCase = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(cases).values({
      companyId: seed.companyId,
      workIssueId: withCase,
      caseNumber: 1,
      identifier: `CASE-${withCase.slice(0, 8)}`,
      caseType: "retention-test",
      title: "Case linked to issue",
    });
    const withDecision = await addIssue(seed, { status: "done", ageDays: 10 });
    const decisionId = randomUUID();
    await db.insert(decisions).values({
      id: decisionId,
      companyId: seed.companyId,
      originAgentId: seed.agentId,
      originIssueId: withDecision,
      originRunId: seed.runId,
      title: "Decision",
      body: "Body",
      options: [],
      expiresAt: seed.now,
      signedSpec: "{}",
      targetSnapshots: {},
    });
    await db.insert(decisionTargetIssues).values({
      decisionId,
      issueId: withDecision,
      companyId: seed.companyId,
    });
    const withPendingApproval = await addIssue(seed, { status: "done", ageDays: 10 });
    const approvalId = randomUUID();
    await db.insert(approvals).values({
      id: approvalId,
      companyId: seed.companyId,
      type: "request_board_approval",
      status: "pending",
      payload: {},
    });
    await db.insert(issueApprovals).values({
      companyId: seed.companyId,
      issueId: withPendingApproval,
      approvalId,
    });
    const withActiveRecovery = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(issueRecoveryActions).values({
      companyId: seed.companyId,
      sourceIssueId: withActiveRecovery,
      kind: "retention-test",
      status: "active",
      cause: "retention-test",
      fingerprint: randomUUID(),
      nextAction: "test",
    });
    const withPendingInteraction = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(issueThreadInteractions).values({
      companyId: seed.companyId,
      issueId: withPendingInteraction,
      kind: "ask_user_questions",
      status: "pending",
      payload: { version: 1, questions: [] } as never,
    });

    const response = await purge(seed, boardActor(seed)).expect(200);
    expect(response.body).toMatchObject({
      policy: "issue-retention.v1",
      retentionDays: 7,
      limit: 100,
      skipped: [],
      hasMore: false,
    });
    expect(response.body.deleted.map((row: { id: string }) => row.id).sort()).toEqual(
      [eligibleDone, eligibleCancelled].sort(),
    );

    const remainingIds = new Set(
      (await db.select({ id: issues.id }).from(issues)).map((row) => row.id),
    );
    expect(remainingIds.has(eligibleDone)).toBe(false);
    expect(remainingIds.has(eligibleCancelled)).toBe(false);
    for (const kept of [
      tooYoung,
      notArchived,
      running,
      monitored,
      checkedOut,
      withOpenChild,
      withTerminalChild,
      withCase,
      withDecision,
      withPendingApproval,
      withActiveRecovery,
      withPendingInteraction,
    ]) {
      expect(remainingIds.has(kept)).toBe(true);
    }

    const audits = await db.select().from(activityLog);
    expect(audits.filter((row) => row.action === "issue.retention_deleted")).toHaveLength(2);
  });

  it("caps the batch, reports hasMore, and lets the routine drain the remaining candidates", async () => {
    const seed = await seedCompany();
    await grant(seed, "issues:retention_delete");
    const ids = [
      await addIssue(seed, { status: "done", ageDays: 30 }),
      await addIssue(seed, { status: "done", ageDays: 20 }),
      await addIssue(seed, { status: "cancelled", ageDays: 10 }),
    ];

    const preview = await purge(seed, boardActor(seed), { limit: 2, dryRun: true }).expect(200);
    expect(preview.body).toMatchObject({ dryRun: true, deleted: [], hasMore: true });
    expect(preview.body.eligible.map((row: { id: string }) => row.id)).toEqual(ids.slice(0, 2));

    const first = await purge(seed, boardActor(seed), { limit: 2 }).expect(200);
    expect(first.body.deleted).toHaveLength(2);
    expect(first.body.hasMore).toBe(true);
    expect(first.body.deleted.map((row: { id: string }) => row.id)).toEqual(ids.slice(0, 2));

    const second = await purge(seed, boardActor(seed), { limit: 2 }).expect(200);
    expect(second.body.deleted.map((row: { id: string }) => row.id)).toEqual([ids[2]]);
    expect(second.body.hasMore).toBe(false);

    const third = await purge(seed, boardActor(seed), { limit: 2 }).expect(200);
    expect(third.body.deleted).toEqual([]);
    expect(third.body.hasMore).toBe(false);
  });

  it("rejects an agent without the dedicated grant even when it holds other task grants", async () => {
    const seed = await seedCompany();
    const eligible = await addIssue(seed, { status: "done", ageDays: 10 });
    await grant(seed, "tasks:manage_active_checkouts");

    const denied = await purge(seed, agentActor(seed)).expect(403);
    expect(denied.body.error).toContain("issues:retention_delete");
    expect(
      (await db.select({ id: issues.id }).from(issues)).map((row) => row.id),
    ).toContain(eligible);

    await grant(seed, "issues:retention_delete");
    const allowed = await purge(seed, agentActor(seed)).expect(200);
    expect(allowed.body.deleted.map((row: { id: string }) => row.id)).toEqual([eligible]);
  });

  it("refuses to shorten the retention window below the server floor", async () => {
    const seed = await seedCompany();
    await grant(seed, "issues:retention_delete");
    const tooYoung = await addIssue(seed, { status: "done", ageDays: 5 });

    const rejected = await purge(seed, boardActor(seed), { retentionDays: 5 });
    expect([400, 422]).toContain(rejected.status);
    expect(
      (await db.select({ id: issues.id }).from(issues)).map((row) => row.id),
    ).toContain(tooYoung);

    const longer = await purge(seed, boardActor(seed), { retentionDays: 30 }).expect(200);
    expect(longer.body.deleted).toEqual([]);
    expect(
      (await db.select({ id: issues.id }).from(issues)).map((row) => row.id),
    ).toContain(tooYoung);

    const dryRun = await purge(seed, boardActor(seed), { dryRun: true }).expect(200);
    expect(dryRun.body).toMatchObject({ dryRun: true, deleted: [] });
    expect(dryRun.body.eligible.map((row: { id: string }) => row.id)).toEqual([]);
  });
});
