CREATE TABLE "issue_attachment_cleanup_outbox" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"company_id" uuid NOT NULL,
	"issue_id" uuid NOT NULL,
	"asset_id" uuid NOT NULL,
	"object_key" text NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"last_attempt_at" timestamp with time zone,
	"last_error" text,
	"completed_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "issue_attachment_cleanup_outbox_status_check" CHECK ("issue_attachment_cleanup_outbox"."status" IN ('pending', 'done'))
);
--> statement-breakpoint
ALTER TABLE "issue_attachment_cleanup_outbox" ADD CONSTRAINT "issue_attachment_cleanup_outbox_company_id_companies_id_fk" FOREIGN KEY ("company_id") REFERENCES "public"."companies"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "issue_attachment_cleanup_outbox_asset_uq" ON "issue_attachment_cleanup_outbox" USING btree ("company_id","asset_id");--> statement-breakpoint
CREATE INDEX "issue_attachment_cleanup_outbox_pending_idx" ON "issue_attachment_cleanup_outbox" USING btree ("status","created_at");