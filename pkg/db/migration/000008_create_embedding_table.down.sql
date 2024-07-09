BEGIN;
-- Drop indexes
DROP INDEX IF EXISTS idx_unique_source;

-- Drop the table embedding
DROP TABLE IF EXISTS embedding;

COMMIT;
