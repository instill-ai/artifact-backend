BEGIN;

-- Drop the index (if it was created in the up migration)
DROP INDEX IF EXISTS idx_knowledge_base_file_size;

-- Remove the column
ALTER TABLE knowledge_base_file DROP COLUMN IF EXISTS size;

COMMIT;
