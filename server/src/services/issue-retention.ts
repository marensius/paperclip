import { and, asc, eq, inArray, isNull, sql, type SQL } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import {
  approvals,
  caseAccessSources,
  caseHandoffs,
  caseIssueLinks,
  cases,
  chatTaskHandoffs,
  decisionEffectExecutions,
  decisionTargetIssues,
  decisionTrainingExamples,
  decisions,
  issueApprovals,
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
  toolActionDeliveries,
} from "@paperclipai/db";

/**
 * The retention policy only ever removes archived, terminal issues that are at
 * least this old. The value is a server-side floor: callers may ask for a
 * longer window but can never shorten it.
 */
export const ISSUE_RETENTION_MIN_DAYS = 7;
export const ISSUE_RETENTION_MAX_DAYS = 365;
export const ISSUE_RETENTION_MAX_BATCH = 100;
const DAY_MS = 86_400_000;

export type IssueRetentionCandidate = {
  id: string;
  identifier: string | null;
  status: "done" | "cancelled";
  terminalAt: Date;
  archivedAt: Date;
};

function normalizeRetentionDays(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return ISSUE_RETENTION_MIN_DAYS;
  return Math.min(
    ISSUE_RETENTION_MAX_DAYS,
    Math.max(ISSUE_RETENTION_MIN_DAYS, Math.trunc(value)),
  );
}

function normalizeBatchLimit(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return ISSUE_RETENTION_MAX_BATCH;
  return Math.min(ISSUE_RETENTION_MAX_BATCH, Math.max(1, Math.trunc(value)));
}

export function issueRetentionCutoff(now: Date, retentionDays?: number | null) {
  return new Date(now.getTime() - normalizeRetentionDays(retentionDays) * DAY_MS);
}

export function issueRetentionBatchLimit(limit?: number | null) {
  return normalizeBatchLimit(limit);
}

function terminalAtExpression() {
  return sql<Date>`case
    when ${issues.status} = 'cancelled' then coalesce(${issues.cancelledAt}, ${issues.updatedAt})
    else coalesce(${issues.completedAt}, ${issues.updatedAt})
  end`;
}

function archivedAtExpression(companyId: string) {
  return sql<Date>`(
    select max(a.archived_at)
    from ${issueInboxArchives} a
    where a.company_id = ${companyId} and a.issue_id = ${sql.raw('"issues"."id"')}
  )`;
}

/**
 * The single source of truth for retention eligibility. Both the batch
 * selection and the in-transaction delete guard use this predicate, so a row
 * that becomes ineligible after selection (reopen, checkout, new reference,
 * fresh terminal timestamp) fails the delete instead of slipping through.
 *
 * The reference checks are intentionally fail-closed for relationships where
 * deleting the issue would cascade into, or detach it from, another record.
 * References owned by the issue itself (comments, documents, labels, its own
 * mentions, its own approvals) are allowed to cascade.
 */
export function retentionEligibilityCondition(companyId: string, cutoff: Date): SQL {
  const terminalAt = terminalAtExpression();
  return and(
    eq(issues.companyId, companyId),
    inArray(issues.status, ["done", "cancelled"]),
    isNull(issues.checkoutRunId),
    isNull(issues.executionRunId),
    isNull(issues.monitorNextCheckAt),
    sql`${terminalAt} <= ${cutoff.toISOString()}::timestamptz`,
    sql`exists (
      select 1 from ${issueInboxArchives} a
      where a.company_id = ${companyId} and a.issue_id = ${issues.id}
    )`,
    // A parent row is referenced by its children without a delete policy,
    // so any child (open or terminal) blocks the delete. Skipping the
    // whole set is intentional: retention must never cascade a tree.
    sql`not exists (
      select 1 from ${issues} child where child.parent_id = ${issues.id}
    )`,
    // Privacy ancestry controls authorization. Deleting an ancestor would
    // SET NULL on the child and widen its visibility, so fail closed.
    sql`not exists (
      select 1 from ${issues} privacy_child
      where privacy_child.privacy_parent_issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${issues} privacy_root
      where privacy_root.privacy_root_issue_id = ${issues.id}
    )`,
    // Blocking edges cascade from either side. Deleting a terminal blocker
    // must not silently remove a live dependency edge from another issue.
    sql`not exists (
      select 1 from ${issueRelations} r
      where r.issue_id = ${issues.id} or r.related_issue_id = ${issues.id}
    )`,
    // Tree holds and watchdog bindings cascade or detach another workflow.
    sql`not exists (
      select 1 from ${issueTreeHolds} h where h.root_issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${issueTreeHoldMembers} m
      where m.issue_id = ${issues.id} or m.parent_issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${issueWatchdogs} w
      where w.issue_id = ${issues.id} or w.watchdog_issue_id = ${issues.id}
    )`,
    // Case, decision, and pipeline links are references owned by another
    // record, not by this issue.
    sql`not exists (
      select 1 from ${cases} c where c.work_issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${caseIssueLinks} l where l.issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${caseHandoffs} h where h.issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${caseAccessSources} s
      where s.source_kind = 'issue' and s.source_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${pipelineCaseIssueLinks} pl where pl.issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${decisionTargetIssues} dt where dt.issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${decisions} d where d.origin_issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${decisionEffectExecutions} de where de.target_issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${decisionTrainingExamples} t where t.issue_id = ${issues.id}
    )`,
    // Chat/tool delivery state and cross-issue mentions are owned elsewhere.
    sql`not exists (
      select 1 from ${chatTaskHandoffs} ch
      where ch.task_id = ${issues.id} or ch.conversation_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${toolActionDeliveries} td where td.issue_id = ${issues.id}
    )`,
    sql`not exists (
      select 1 from ${issueReferenceMentions} rm where rm.target_issue_id = ${issues.id}
    )`,
    // Open workflow state still attached to the issue.
    sql`not exists (
      select 1 from ${issueRecoveryActions} r
      where r.source_issue_id = ${issues.id} and r.status in ('active', 'escalated')
    )`,
    sql`not exists (
      select 1 from ${issueThreadInteractions} i
      where i.issue_id = ${issues.id} and i.status = 'pending'
    )`,
    sql`not exists (
      select 1 from ${issueApprovals} ap
      join ${approvals} a on a.id = ap.approval_id
      where ap.issue_id = ${issues.id} and a.status in ('pending', 'revision_requested')
    )`,
  )!;
}

/**
 * Server-owned retention eligibility. Callers cannot name a specific issue:
 * this query decides which issues the retention policy may purge. It is the
 * only source of truth for hard-delete eligibility, so a compromised sandbox
 * caller cannot widen the set.
 */
export function issueRetentionService(db: Db) {
  async function listEligible(input: {
    companyId: string;
    cutoff: Date;
    limit?: number | null;
  }): Promise<{ candidates: IssueRetentionCandidate[]; hasMore: boolean }> {
    const limit = normalizeBatchLimit(input.limit);
    const terminalAt = terminalAtExpression();
    const archivedAt = archivedAtExpression(input.companyId);
    const rows = await db
      .select({
        id: issues.id,
        identifier: issues.identifier,
        status: issues.status,
        terminalAt,
        archivedAt,
      })
      .from(issues)
      .where(retentionEligibilityCondition(input.companyId, input.cutoff))
      .orderBy(asc(terminalAt), asc(issues.id))
      .limit(limit + 1);

    const hasMore = rows.length > limit;
    const candidates = rows.slice(0, limit).map((row) => ({
      ...row,
      status: row.status as IssueRetentionCandidate["status"],
    }));
    return { candidates, hasMore };
  }

  return { listEligible };
}
