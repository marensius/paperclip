import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";
import { companies } from "./companies.js";

/**
 * Durable outbox for object-store cleanup of issue attachments after a hard
 * delete commits. The row is written in the same transaction as the issue
 * delete, so an object is only ever removed after the referencing issue and
 * attachment rows are already gone. Object deletion is idempotent and retried
 * from pending rows, which leaves orphaned objects retryable instead of
 * leaving a live issue that points at missing objects.
 */
export const issueAttachmentCleanupOutbox = pgTable(
  "issue_attachment_cleanup_outbox",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    companyId: uuid("company_id")
      .notNull()
      .references(() => companies.id, { onDelete: "cascade" }),
    issueId: uuid("issue_id").notNull(),
    assetId: uuid("asset_id").notNull(),
    objectKey: text("object_key").notNull(),
    status: text("status").notNull().default("pending"),
    attemptCount: integer("attempt_count").notNull().default(0),
    lastAttemptAt: timestamp("last_attempt_at", { withTimezone: true }),
    lastError: text("last_error"),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => ({
    assetUq: uniqueIndex("issue_attachment_cleanup_outbox_asset_uq").on(
      table.companyId,
      table.assetId,
    ),
    pendingIdx: index("issue_attachment_cleanup_outbox_pending_idx").on(
      table.status,
      table.createdAt,
    ),
    statusCheck: check(
      "issue_attachment_cleanup_outbox_status_check",
      sql`${table.status} IN ('pending', 'done')`,
    ),
  }),
);
