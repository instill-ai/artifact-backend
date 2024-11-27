BEGIN;
-- Remove catalog_type column from knowledge_base table
ALTER TABLE knowledge_base DROP COLUMN catalog_type;
COMMIT;
