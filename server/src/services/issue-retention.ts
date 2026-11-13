import { and, asc, eq, inArray, isNull, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import {
  approvals,
  caseAccessSources,
  caseHandoffs,
  caseIssueLinks,
  cases,
  decisionTargetIssues,
  decisions,
  issueApprovals,
  issueInboxArchives,
  issueRecoveryActions,
  issueThreadInteractions,
  issues,
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
      .where(
        and(
          eq(issues.companyId, input.companyId),
          inArray(issues.status, ["done", "cancelled"]),
          isNull(issues.checkoutRunId),
          isNull(issues.executionRunId),
          isNull(issues.monitorNextCheckAt),
          sql`${terminalAt} <= ${input.cutoff.toISOString()}::timestamptz`,
          sql`exists (
            select 1 from ${issueInboxArchives} a
            where a.company_id = ${input.companyId} and a.issue_id = ${issues.id}
          )`,
          // A parent row is referenced by its children without a delete policy,
          // so any child (open or terminal) blocks the delete. Skipping the
          // whole set is intentional: retention must never cascade a tree.
          sql`not exists (
            select 1 from ${issues} child where child.parent_id = ${issues.id}
          )`,
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
            select 1 from ${decisionTargetIssues} dt where dt.issue_id = ${issues.id}
          )`,
          sql`not exists (
            select 1 from ${decisions} d where d.origin_issue_id = ${issues.id}
          )`,
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
        ),
      )
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
