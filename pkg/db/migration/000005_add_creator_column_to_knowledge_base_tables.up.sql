BEGIN;
-- Add the new column to the knowledge_base table
ALTER TABLE knowledge_base
ADD COLUMN creator_uid UUID NOT NULL;

-- Add comment for the new column
COMMENT ON COLUMN knowledge_base.creator_uid IS 'UUID of the creator of the knowledge base';

COMMIT;
