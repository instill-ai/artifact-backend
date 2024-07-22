BEGIN;
-- Drop the indexes
DROP INDEX IF EXISTS idx_embedding_kb_uid;
DROP INDEX IF EXISTS idx_embedding_kb_file_uid;

-- Drop the columns
ALTER TABLE embedding
    DROP COLUMN IF EXISTS kb_uid,
    DROP COLUMN IF EXISTS kb_file_uid;
COMMIT;
