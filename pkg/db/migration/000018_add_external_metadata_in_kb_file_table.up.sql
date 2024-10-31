BEGIN;

-- Add external_metadata column to knowledge_base_file table
ALTER TABLE knowledge_base_file
ADD COLUMN external_metadata JSONB;

-- Add comment for the new column
COMMENT ON COLUMN knowledge_base_file.external_metadata IS 'External metadata stored as JSON, serialized from protobuf Struct';

COMMIT;
