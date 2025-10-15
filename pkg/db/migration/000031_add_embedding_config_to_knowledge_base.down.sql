BEGIN;
-- Drop the index
DROP INDEX IF EXISTS idx_knowledge_base_embedding_model_family;
-- Remove the embedding_config column
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS embedding_config;
COMMIT;
