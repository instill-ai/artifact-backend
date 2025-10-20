BEGIN;
-- Add staging boolean flag to knowledge_base
-- staging=false: Production KB (actively used for queries)
-- staging=true: Staging/rollback KB (held for N days for potential rollback)
ALTER TABLE knowledge_base
ADD COLUMN staging BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX idx_kb_staging ON knowledge_base(staging);
-- Add upgrade tracking fields to knowledge_base
ALTER TABLE knowledge_base
ADD COLUMN update_status VARCHAR(50);
ALTER TABLE knowledge_base
ADD COLUMN update_workflow_id VARCHAR(255);
ALTER TABLE knowledge_base
ADD COLUMN update_started_at TIMESTAMP;
ALTER TABLE knowledge_base
ADD COLUMN update_completed_at TIMESTAMP;
ALTER TABLE knowledge_base
ADD COLUMN rollback_retention_until TIMESTAMP;
-- Add active_collection_uid to knowledge_base to support independent collection versioning
-- This allows KBs to point to collections with different embedding dimensionalities
-- During updates, a NEW collection is ALWAYS created for the staging KB (even if dimensions don't change)
-- This ensures clean isolation between staging and production data during upgrades
-- The swap operation updates active_collection_uid pointers (not collections themselves)
-- The KB UID remains constant, but active_collection_uid points to the correct collection
ALTER TABLE knowledge_base
ADD COLUMN active_collection_uid UUID;
-- For existing KBs, set active_collection_uid to their own UID (legacy behavior)
-- New KBs will have this set explicitly during creation
UPDATE knowledge_base
SET active_collection_uid = uid
WHERE active_collection_uid IS NULL;
-- Create index for faster lookups
CREATE INDEX idx_kb_active_collection_uid ON knowledge_base(active_collection_uid);
COMMENT ON COLUMN knowledge_base.active_collection_uid IS 'UUID of the Milvus collection currently used by this KB. Allows collection versioning for dimension changes.';
-- Create system table for system-wide configuration profiles
-- This table stores complete configuration profiles as nested JSON
CREATE TABLE IF NOT EXISTS system (
    profile VARCHAR(255) PRIMARY KEY DEFAULT 'default',
    config JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_system_updated_at ON system(updated_at);
COMMENT ON TABLE system IS 'System-wide configuration profiles';
COMMENT ON COLUMN system.profile IS 'Profile name (default, free, enterprise, custom)';
COMMENT ON COLUMN system.config IS 'Complete system configuration as nested JSON';
COMMENT ON COLUMN system.description IS 'Description of this configuration profile';
-- Initialize with default profile matching existing KBs (OpenAI/1536 from migration 000031)
-- This ensures new KBs created during migration use the same config as existing KBs
INSERT INTO system (profile, config, description)
VALUES (
        'default',
        '{
      "rag": {
        "default_embedding_config": {
          "model_family": "openai",
          "dimensionality": 1536
        }
      }
    }'::jsonb,
        'Default system configuration profile'
    ) ON CONFLICT (profile) DO NOTHING;
COMMIT;
