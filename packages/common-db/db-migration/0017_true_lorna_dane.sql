CREATE TABLE "call_data"."api_response_deletion" (
	"response_id" uuid NOT NULL,
	"organization_id" text NOT NULL,
	"deleted_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "api_response_deletion_pkey" PRIMARY KEY ("response_id")
);
--> statement-breakpoint
ALTER TABLE "call_data"."api_response_deletion" ADD CONSTRAINT "api_response_deletion_organization_id_organization_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organization"("id") ON DELETE cascade ON UPDATE no action;