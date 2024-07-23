BEGIN;
-- Add the usage column to the knowledge_base table
ALTER TABLE knowledge_base ADD COLUMN usage INTEGER NOT NULL DEFAULT 0;
-- Add a comment for the new column
COMMENT ON COLUMN knowledge_base.usage IS 'Tracks the usage count of the knowledge base';
COMMIT;
