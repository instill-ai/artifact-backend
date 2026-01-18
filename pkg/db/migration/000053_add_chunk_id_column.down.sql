-- Down migration: Remove ID column from chunk table

DROP INDEX IF EXISTS idx_chunk_file_id;
ALTER TABLE chunk DROP COLUMN IF EXISTS id;
