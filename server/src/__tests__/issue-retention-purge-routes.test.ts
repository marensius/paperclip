import { randomUUID } from "node:crypto";
import express from "express";
import { eq } from "drizzle-orm";
import request from "supertest";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  activityLog,
  agents,
  approvals,
  assets,
  authUsers,
  cases,
  chatTaskHandoffs,
  companies,
  companyMemberships,
  createDb,
  decisionEffectExecutions,
  decisionTargetIssues,
  decisionTrainingExamples,
  decisions,
  heartbeatRuns,
  issueApprovals,
  issueAttachments,
  issueAttachmentCleanupOutbox,
  issueInboxArchives,
  issueRecoveryActions,
  issueReferenceMentions,
  issueRelations,
  issueThreadInteractions,
  issueTreeHoldMembers,
  issueTreeHolds,
  issueWatchdogs,
  issues,
  pipelineCaseIssueLinks,
  principalPermissionGrants,
  toolActionDeliveries,
} from "@paperclipai/db";
import { errorHandler } from "../middleware/index.js";
import { issueRoutes } from "../routes/issues.js";
import { issueRetentionCutoff } from "../services/issue-retention.js";
import { issueService } from "../services/issues.js";
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
    await db.delete(issueAttachmentCleanupOutbox);
    await db.delete(toolActionDeliveries);
    await db.delete(chatTaskHandoffs);
    await db.delete(issueReferenceMentions);
    await db.delete(issueRelations);
    await db.delete(issueTreeHoldMembers);
    await db.delete(issueTreeHolds);
    await db.delete(issueWatchdogs);
    await db.delete(decisionTrainingExamples);
    await db.delete(decisionEffectExecutions);
    await db.delete(decisionTargetIssues);
    await db.delete(decisions);
    await db.delete(pipelineCaseIssueLinks);
    await db.delete(issueThreadInteractions);
    await db.delete(issueRecoveryActions);
    await db.delete(issueApprovals);
    await db.delete(approvals);
    await db.delete(cases);
    await db.delete(activityLog);
    await db.delete(issueAttachments);
    await db.delete(assets);
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

  function appFor(
    actor: Express.Request["actor"],
    storage: Record<string, unknown> = {},
  ) {
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.actor = actor;
      next();
    });
    app.use("/api", issueRoutes(db, storage as never));
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
      archiveActor?: "user" | "agent";
      archiveAgentId?: string | null;
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
      const archiveActor = input.archiveActor ?? "agent";
      await db.insert(issueInboxArchives).values({
        companyId: seed.companyId,
        issueId: id,
        userId: seed.userId,
        archivedByActorType: archiveActor,
        archivedByAgentId:
          archiveActor === "agent"
            ? input.archiveAgentId === undefined
              ? seed.agentId
              : input.archiveAgentId
            : null,
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

    // Cross-issue relationships must fail closed: deleting an old terminal
    // issue must never remove an edge or binding that another workflow owns.
    const withRelationTarget = await addIssue(seed, { status: "done", ageDays: 10 });
    const relationSource = await addIssue(seed, { status: "todo", ageDays: 1, archived: false });
    await db.insert(issueRelations).values({
      companyId: seed.companyId,
      issueId: relationSource,
      relatedIssueId: withRelationTarget,
      type: "blocks",
    });

    const withPrivacyChild = await addIssue(seed, { status: "done", ageDays: 10 });
    await addIssue(seed, {
      status: "todo",
      ageDays: 1,
      archived: false,
      extra: {
        privacyParentIssueId: withPrivacyChild,
        privacyRootIssueId: withPrivacyChild,
      },
    });

    const withTreeHold = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(issueTreeHolds).values({
      companyId: seed.companyId,
      rootIssueId: withTreeHold,
      mode: "retention-test",
      status: "active",
    });

    const withTreeMember = await addIssue(seed, { status: "done", ageDays: 10 });
    const treeHoldRoot = await addIssue(seed, { status: "todo", ageDays: 1, archived: false });
    const treeHoldId = randomUUID();
    await db.insert(issueTreeHolds).values({
      id: treeHoldId,
      companyId: seed.companyId,
      rootIssueId: treeHoldRoot,
      mode: "retention-test",
      status: "active",
    });
    await db.insert(issueTreeHoldMembers).values({
      companyId: seed.companyId,
      holdId: treeHoldId,
      issueId: withTreeMember,
      issueTitle: "Retention member",
      issueStatus: "done",
    });

    const withWatchdog = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(issueWatchdogs).values({
      companyId: seed.companyId,
      issueId: withWatchdog,
      watchdogAgentId: seed.agentId,
      status: "active",
    });

    const withTrainingExample = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(decisionTrainingExamples).values({
      companyId: seed.companyId,
      sourceKind: "interaction",
      sourceId: randomUUID(),
      issueId: withTrainingExample,
      cutoffAt: seed.now,
      snapshot: {} as never,
      createdByUserId: seed.userId,
    });

    const withMentionTarget = await addIssue(seed, { status: "done", ageDays: 10 });
    const mentionSource = await addIssue(seed, { status: "todo", ageDays: 1, archived: false });
    await db.insert(issueReferenceMentions).values({
      companyId: seed.companyId,
      sourceIssueId: mentionSource,
      targetIssueId: withMentionTarget,
      sourceKind: "comment",
    });

    const withChatHandoff = await addIssue(seed, { status: "done", ageDays: 10 });
    const conversationIssue = await addIssue(seed, { status: "todo", ageDays: 1, archived: false });
    await db.insert(chatTaskHandoffs).values({
      taskId: withChatHandoff,
      companyId: seed.companyId,
      conversationId: conversationIssue,
      agentId: seed.agentId,
      sessionGeneration: 1,
    });

    const withDecisionEffect = await addIssue(seed, { status: "done", ageDays: 10 });
    await db.insert(decisionEffectExecutions).values({
      decisionId,
      effectIndex: 1,
      effectType: "retention-test",
      targetIssueId: withDecisionEffect,
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
      withRelationTarget,
      withPrivacyChild,
      withTreeHold,
      withTreeMember,
      withWatchdog,
      withTrainingExample,
      withMentionTarget,
      withChatHandoff,
      withDecisionEffect,
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

  it("re-evaluates the policy under the lock and refuses a re-completed issue", async () => {
    const seed = await seedCompany();
    await grant(seed, "issues:retention_delete");
    const issueId = await addIssue(seed, { status: "done", ageDays: 10 });
    const assetId = randomUUID();
    await db.insert(assets).values({
      id: assetId,
      companyId: seed.companyId,
      provider: "local",
      objectKey: `retention/${assetId}`,
      contentType: "text/plain",
      byteSize: 5,
      sha256: "0".repeat(64),
      createdByUserId: seed.userId,
    });
    await db.insert(issueAttachments).values({
      companyId: seed.companyId,
      issueId,
      assetId,
    });
    const svc = issueService(db);

    // Simulate a reopen and fresh re-completion between selection and delete:
    // the issue is terminal again but no longer older than the cutoff.
    await db
      .update(issues)
      .set({ completedAt: seed.now, updatedAt: seed.now })
      .where(eq(issues.id, issueId));

    await expect(
      svc.remove(issueId, {
        requireRetentionEligible: {
          companyId: seed.companyId,
          cutoff: issueRetentionCutoff(seed.now, 7),
          audit: {
            actorType: "user",
            actorId: seed.userId,
            agentId: null,
            runId: null,
            agentApiKeyId: null,
            retentionDays: 7,
            identifier: null,
            status: "done",
            terminalAt: seed.now.toISOString(),
            archivedAt: seed.now.toISOString(),
            cutoff: issueRetentionCutoff(seed.now, 7).toISOString(),
          },
        },
      }),
    ).rejects.toThrow(/no longer retention-eligible/);

    expect(
      (await db.select({ id: issues.id }).from(issues)).map((row) => row.id),
    ).toContain(issueId);
    // A rolled-back delete must not enqueue any object cleanup.
    expect(await db.select().from(issueAttachmentCleanupOutbox)).toHaveLength(0);
    expect(
      (await db.select().from(issueAttachments)).filter((row) => row.issueId === issueId),
    ).toHaveLength(1);
  });

  it("commits the delete and cleanup outbox before touching object storage, then retries partial cleanup", async () => {
    const seed = await seedCompany();
    await grant(seed, "issues:retention_delete");
    const issueId = await addIssue(seed, { status: "done", ageDays: 10 });
    const assetIds = [randomUUID(), randomUUID()];
    for (const assetId of assetIds) {
      await db.insert(assets).values({
        id: assetId,
        companyId: seed.companyId,
        provider: "local",
        objectKey: `retention/${assetId}`,
        contentType: "text/plain",
        byteSize: 5,
        sha256: "0".repeat(64),
        createdByUserId: seed.userId,
      });
      await db.insert(issueAttachments).values({
        companyId: seed.companyId,
        issueId,
        assetId,
      });
    }
    const objectKeys = assetIds.map((assetId) => `retention/${assetId}`).sort();

    // Fail the second object deletion to model a partial multi-attachment
    // cleanup. The issue delete and the outbox rows have already committed,
    // so no live issue can be left pointing at a missing object.
    const attempted: string[] = [];
    const partial = await request(
      appFor(boardActor(seed), {
        deleteObject: async (_companyId: string, objectKey: string) => {
          attempted.push(objectKey);
          if (attempted.length === 2) throw new Error("storage unavailable");
        },
      }),
    )
      .post(`/api/companies/${seed.companyId}/issues/retention-purge`)
      .send({})
      .expect(200);
    expect(partial.body.deleted.map((row: { id: string }) => row.id)).toEqual([issueId]);
    expect(partial.body.skipped).toEqual([]);
    expect(partial.body.attachmentCleanup).toEqual({
      completed: 1,
      failed: 1,
      pending: 1,
    });
    expect([...attempted].sort()).toEqual(objectKeys);

    const remainingIssues = await db.select({ id: issues.id }).from(issues);
    expect(remainingIssues.map((row) => row.id)).not.toContain(issueId);
    expect(
      (await db.select().from(issueAttachments)).filter((row) => row.issueId === issueId),
    ).toHaveLength(0);
    expect(
      (await db.select().from(assets)).filter((row) => assetIds.includes(row.id)),
    ).toHaveLength(0);
    expect(
      (await db.select().from(activityLog)).filter(
        (row) => row.action === "issue.retention_deleted",
      ),
    ).toHaveLength(1);

    const outbox = await db.select().from(issueAttachmentCleanupOutbox);
    expect(outbox).toHaveLength(2);
    expect(outbox.filter((row) => row.status === "done")).toHaveLength(1);
    const pending = outbox.filter((row) => row.status === "pending");
    expect(pending).toHaveLength(1);
    expect(pending[0]?.lastError).toContain("storage unavailable");

    // A later purge pass retries the pending object without re-deleting the
    // (already gone) issue. The object deletion is idempotent.
    const retried: string[] = [];
    const retry = await request(
      appFor(boardActor(seed), {
        deleteObject: async (_companyId: string, objectKey: string) => {
          retried.push(objectKey);
        },
      }),
    )
      .post(`/api/companies/${seed.companyId}/issues/retention-purge`)
      .send({})
      .expect(200);
    expect(retry.body.deleted).toEqual([]);
    expect(retry.body.attachmentCleanup).toEqual({
      completed: 1,
      failed: 0,
      pending: 0,
    });
    expect(retried).toHaveLength(1);
    expect(objectKeys).toContain(retried[0]);
    expect(
      (await db.select().from(issueAttachmentCleanupOutbox)).every(
        (row) => row.status === "done",
      ),
    ).toBe(true);
  });

  it("requires an agent-attributed archive from an identity holding the retention grant", async () => {
    const seed = await seedCompany();
    await grant(seed, "issues:retention_delete");
    const otherAgentId = randomUUID();
    await db.insert(agents).values({
      id: otherAgentId,
      companyId: seed.companyId,
      name: "Unrelated Agent",
      role: "engineer",
      status: "active",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    });

    const designated = await addIssue(seed, { status: "done", ageDays: 10 });
    // A user's personal inbox archive is not a retention signal: it must not
    // make the issue deletable for every other user in the company.
    const userArchived = await addIssue(seed, {
      status: "done",
      ageDays: 12,
      archiveActor: "user",
    });
    // An agent archive only counts for the identity that holds the dedicated
    // retention grant, not for every agent that can manage an inbox.
    const agentWithoutGrant = await addIssue(seed, {
      status: "done",
      ageDays: 8,
      archiveActor: "agent",
      archiveAgentId: otherAgentId,
    });

    const response = await purge(seed, boardActor(seed)).expect(200);
    expect(response.body.deleted.map((row: { id: string }) => row.id)).toEqual([designated]);

    const remaining = new Set(
      (await db.select({ id: issues.id }).from(issues)).map((row) => row.id),
    );
    expect(remaining.has(designated)).toBe(false);
    expect(remaining.has(userArchived)).toBe(true);
    expect(remaining.has(agentWithoutGrant)).toBe(true);
  });
});
