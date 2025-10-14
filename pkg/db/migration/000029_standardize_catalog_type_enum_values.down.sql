BEGIN;
-- Revert catalog_type values to legacy format
-- This down migration is provided for rollback compatibility
UPDATE knowledge_base
SET catalog_type = 'persistent'
WHERE catalog_type = 'CATALOG_TYPE_PERSISTENT';
UPDATE knowledge_base
SET catalog_type = 'ephemeral'
WHERE catalog_type = 'CATALOG_TYPE_EPHEMERAL';
-- Restore original comment
COMMENT ON COLUMN knowledge_base.catalog_type IS 'Type of the knowledge base catalog';
COMMIT;
