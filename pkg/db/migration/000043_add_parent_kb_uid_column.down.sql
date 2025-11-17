BEGIN;

-- Drop the parent_kb_uid column and its index
DROP INDEX IF EXISTS idx_kb_parent_kb_uid;

ALTER TABLE knowledge_base
DROP COLUMN IF EXISTS parent_kb_uid;

COMMIT;
