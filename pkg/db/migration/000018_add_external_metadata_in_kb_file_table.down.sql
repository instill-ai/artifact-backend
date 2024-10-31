BEGIN;

-- Remove external_metadata column from knowledge_base_file table
ALTER TABLE knowledge_base_file
DROP COLUMN external_metadata;

COMMIT;
