BEGIN;
-- Add embedding_config column as JSONB to store AI model configuration
-- Format: {"model_family": "openai", "dimensionality": 1536}
ALTER TABLE knowledge_base
ADD COLUMN IF NOT EXISTS embedding_config JSONB;
-- Set default values for existing records (OpenAI with 1536 dimensions)
UPDATE knowledge_base
SET embedding_config = '{"model_family": "openai", "dimensionality": 1536}'::jsonb
WHERE embedding_config IS NULL;
-- Make it NOT NULL after populating existing records
ALTER TABLE knowledge_base
ALTER COLUMN embedding_config
SET NOT NULL;
-- Add comment explaining the structure
COMMENT ON COLUMN knowledge_base.embedding_config IS 'Embedding configuration for the catalog (JSON fields: model_family, dimensionality)';
-- Create an index on the model_family field for efficient querying
CREATE INDEX IF NOT EXISTS idx_knowledge_base_embedding_model_family ON knowledge_base ((embedding_config->>'model_family'));
COMMIT;
