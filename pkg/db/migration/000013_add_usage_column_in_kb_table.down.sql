BEGIN;
-- Remove the usage column from the knowledge_base table
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS usage;
COMMIT;
