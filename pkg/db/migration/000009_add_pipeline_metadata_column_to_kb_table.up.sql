BEGIN;

-- Add the new column 'pipelines' to the 'knowledge_base' table
ALTER TABLE knowledge_base
ADD COLUMN pipelines JSONB;

-- Add a comment for the new 'pipelines' column
COMMENT ON COLUMN knowledge_base.pipelines IS 'JSONB column to store pipeline configurations or data related to the knowledge base';

COMMIT;
