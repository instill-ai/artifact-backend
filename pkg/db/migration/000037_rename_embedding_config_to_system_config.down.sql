BEGIN;
-- Revert migration: Convert system_uid foreign key back to embedding_config JSON
-- Step 0: Remove update tracking columns
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS update_error_message;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS previous_system_uid;
-- Step 1: Add back embedding_config column
ALTER TABLE knowledge_base
ADD COLUMN embedding_config JSONB;
-- Step 2: Populate embedding_config from system table (flatten from nested structure)
UPDATE knowledge_base kb
SET embedding_config = s.config->'rag'->'embedding'
FROM system s
WHERE kb.system_uid = s.uid;
-- Step 3: Make embedding_config NOT NULL
ALTER TABLE knowledge_base
ALTER COLUMN embedding_config
SET NOT NULL;
-- Step 4: Recreate the old index
CREATE INDEX IF NOT EXISTS idx_knowledge_base_embedding_model_family ON knowledge_base ((embedding_config->>'model_family'));
-- Step 5: Add back the old comment
COMMENT ON COLUMN knowledge_base.embedding_config IS 'Embedding configuration for the catalog (JSON fields: model_family, dimensionality)';
-- Step 6: Drop the foreign key constraint
ALTER TABLE knowledge_base DROP CONSTRAINT IF EXISTS fk_knowledge_base_system;
-- Step 7: Drop the system_uid column
DROP INDEX IF EXISTS idx_knowledge_base_system_uid;
ALTER TABLE knowledge_base DROP COLUMN system_uid;
-- Step 8: Revert system table changes - create old-style system table
-- Note: is_default column is dropped as it didn't exist in the old schema
CREATE TABLE IF NOT EXISTS system_old (
    profile VARCHAR(255) PRIMARY KEY DEFAULT 'default',
    config JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- Step 9: Migrate data back to old format (flatten config and use id as profile)
INSERT INTO system_old (profile, config, description, updated_at)
SELECT id as profile,
    config->'rag'->'embedding' as config,
    -- Flatten back to old structure
    description,
    update_time as updated_at
FROM system
WHERE delete_time IS NULL;
-- Step 10: Drop new table and rename old one
DROP TABLE system;
ALTER TABLE system_old
    RENAME TO system;
-- Step 11: Restore indexes
CREATE INDEX idx_system_updated_at ON system(updated_at);
-- Step 12: Add original comments
COMMENT ON TABLE system IS 'System-wide configuration profiles';
COMMENT ON COLUMN system.profile IS 'Profile name (default, free, enterprise, custom)';
COMMENT ON COLUMN system.config IS 'Complete system configuration as nested JSON';
COMMENT ON COLUMN system.description IS 'Description of this configuration profile';
COMMIT;
