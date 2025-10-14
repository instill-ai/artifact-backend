BEGIN;
-- Update legacy catalog_type values to use full enum names for consistency
-- This ensures all records use the protobuf enum string representation
UPDATE knowledge_base
SET catalog_type = 'CATALOG_TYPE_PERSISTENT'
WHERE catalog_type = 'persistent';
UPDATE knowledge_base
SET catalog_type = 'CATALOG_TYPE_EPHEMERAL'
WHERE catalog_type = 'ephemeral';
-- Add comment explaining the format
COMMENT ON COLUMN knowledge_base.catalog_type IS 'Type of the knowledge base catalog (stored as protobuf enum string, e.g., CATALOG_TYPE_PERSISTENT)';
COMMIT;
