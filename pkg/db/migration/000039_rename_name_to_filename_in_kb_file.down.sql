BEGIN;
-- 2. Restore the 'name' column in knowledge_base table for rollback purposes
-- Note: After rollback, the column will be empty and needs to be populated
-- with kb_id values manually if needed
ALTER TABLE knowledge_base
ADD COLUMN IF NOT EXISTS name VARCHAR(255);
-- Set name = id for existing records
UPDATE knowledge_base
SET name = id
WHERE name IS NULL
    OR name = '';
-- Make it NOT NULL after populating
ALTER TABLE knowledge_base
ALTER COLUMN name
SET NOT NULL;
-- 1. Revert: Rename filename back to name in knowledge_base_file table
ALTER TABLE knowledge_base_file
    RENAME COLUMN filename TO name;
-- Revert comment
COMMENT ON COLUMN knowledge_base_file.name IS 'Name of the file';
-- Note: The unique index was already dropped in migration 000023
-- No index rename needed
COMMIT;
