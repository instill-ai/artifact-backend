BEGIN;

-- Add catalog_type column to knowledge_base table
ALTER TABLE knowledge_base
ADD COLUMN catalog_type VARCHAR(255) DEFAULT 'CATALOG_TYPE_PERSISTENT';

-- Add comment for the new column
COMMENT ON COLUMN knowledge_base.catalog_type IS 'Type of the knowledge base catalog';

COMMIT;
