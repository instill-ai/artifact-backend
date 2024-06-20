BEGIN;
-- Remove the comment on the creator_uid column
COMMENT ON COLUMN knowledge_base.creator_uid IS NULL;
-- Drop the creator_uid column from the knowledge_base table
ALTER TABLE knowledge_base DROP COLUMN creator_uid;
COMMIT;