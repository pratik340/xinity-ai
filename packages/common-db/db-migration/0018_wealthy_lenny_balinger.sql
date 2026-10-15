/*
    Unfortunately in current drizzle-kit version we can't automatically get name for primary key.
    We are working on making it available!

    Meanwhile you can:
        1. Check pk name in your database, by running
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_schema = 'call_data'
                AND table_name = 'api_response_deletion'
                AND constraint_type = 'PRIMARY KEY';
        2. Uncomment code below and paste pk name manually

    Hope to release this update as soon as possible
*/

ALTER TABLE "call_data"."api_response_deletion" DROP CONSTRAINT "api_response_deletion_pkey";
--> statement-breakpoint
ALTER TABLE "call_data"."api_response_deletion" ADD CONSTRAINT "api_response_deletion_organization_id_response_id_pk" PRIMARY KEY("organization_id","response_id");
