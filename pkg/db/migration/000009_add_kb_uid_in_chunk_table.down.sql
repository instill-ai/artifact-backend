BEGIN;

-- Drop the index
DROP INDEX IF EXISTS idx_text_chunk_kb_uid;

-- Remove the column
ALTER TABLE text_chunk DROP COLUMN IF EXISTS kb_uid;

COMMIT;
