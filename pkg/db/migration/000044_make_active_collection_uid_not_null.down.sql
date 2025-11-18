-- Rollback: Revert active_collection_uid constraints

-- Remove unique constraint
DROP INDEX IF EXISTS idx_kb_active_collection_uid_unique;

-- Remove NOT NULL constraint
ALTER TABLE knowledge_base
ALTER COLUMN active_collection_uid DROP NOT NULL;

-- Revert to old behavior (set to KB UID for production KBs)
-- Note: This is for backward compatibility only, new code should not rely on this
UPDATE knowledge_base
SET active_collection_uid = uid
WHERE staging = false;

-- Remove comment
COMMENT ON COLUMN knowledge_base.active_collection_uid IS NULL;
