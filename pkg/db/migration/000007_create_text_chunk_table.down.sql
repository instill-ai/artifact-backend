BEGIN;
-- Drop indexes
DROP INDEX IF EXISTS idx_unique_source_uid_start_end;
DROP INDEX IF EXISTS idx_source_uid;
-- Drop the table text_chunk
DROP TABLE IF EXISTS text_chunk;
COMMIT;
