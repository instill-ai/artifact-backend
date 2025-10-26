BEGIN;
-- Ensure uuid-ossp extension is available for uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Part 1: Migrate knowledge_base from embedding_config JSON to system_uid foreign key
-- This follows the standard pattern: {referenced_table}_uid for foreign key columns
-- Step 1.1: Refactor system table first (moved from Part 2)
-- Add uid (primary key), id (user-facing identifier), timestamps
-- Systems are global resources (no owner field, resource name is computed as systems/{id})
CREATE TABLE IF NOT EXISTS system_new (
    uid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    id VARCHAR(255) NOT NULL UNIQUE,
    -- User-facing ID (e.g., "openai", "gemini")
    config JSONB NOT NULL,
    description TEXT,
    is_default BOOLEAN NOT NULL DEFAULT false,
    -- Indicates if this is the default system configuration
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP -- Soft delete support
);
-- Step 1.2: Migrate existing data from old system table
INSERT INTO system_new (
        uid,
        id,
        config,
        description,
        create_time,
        update_time
    )
SELECT uuid_generate_v4() as uid,
    profile as id,
    -- Migrate old profile column to new id column
    jsonb_build_object(
        'rag',
        jsonb_build_object('embedding', config)
    ) as config,
    -- Transform to nested structure
    description,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as create_time,
    COALESCE(updated_at, CURRENT_TIMESTAMP) as update_time
FROM system;
-- Step 1.3: Drop old table and rename new one
DROP TABLE system;
ALTER TABLE system_new
    RENAME TO system;
-- Step 1.4: Create indexes on system table
CREATE INDEX idx_system_id ON system(id);
CREATE INDEX idx_system_update_time ON system(update_time);
CREATE INDEX idx_system_delete_time ON system(delete_time)
WHERE delete_time IS NOT NULL;
-- Partial unique index to enforce only one default system
-- This prevents multiple systems from being set as default simultaneously
CREATE UNIQUE INDEX idx_system_is_default ON system(is_default)
WHERE is_default = true
    AND delete_time IS NULL;
-- Step 1.5: Add comments on system table
COMMENT ON TABLE system IS 'System-wide configurations for Knowledge Base System';
COMMENT ON COLUMN system.uid IS 'Unique identifier (UUID) - primary key';
COMMENT ON COLUMN system.id IS 'User-facing identifier conforming to RFC-1034 (e.g., "openai", "gemini"). Resource name is computed as systems/{id}';
COMMENT ON COLUMN system.config IS 'Complete system configuration as nested JSON (rag.embedding.{model_family, dimensionality})';
COMMENT ON COLUMN system.description IS 'Description of this system configuration';
COMMENT ON COLUMN system.is_default IS 'Indicates if this is the default system configuration used when creating new knowledge bases. Only one system can be default at a time (enforced by partial unique index)';
-- Step 1.6: Ensure standard system configurations exist
-- Create standard system configs that are commonly used
-- Use ON CONFLICT DO UPDATE to overwrite any incorrectly migrated configs from step 1.2
INSERT INTO system (uid, id, config, description, is_default)
VALUES (
        uuid_generate_v4(),
        'openai',
        '{"rag": {"embedding": {"model_family": "openai", "dimensionality": 1536}}}'::jsonb,
        'OpenAI embedding configuration (1536 dimensions)',
        true -- OpenAI is the default system for existing and new knowledge bases
    ),
    (
        uuid_generate_v4(),
        'gemini',
        '{"rag": {"embedding": {"model_family": "gemini", "dimensionality": 3072}}}'::jsonb,
        'Google Gemini embedding configuration (3072 dimensions)',
        false
    ) ON CONFLICT (id) DO
UPDATE
SET config = EXCLUDED.config,
    description = EXCLUDED.description,
    is_default = EXCLUDED.is_default,
    update_time = CURRENT_TIMESTAMP;
-- Step 1.7: Create system records for each unique embedding_config in knowledge_base
-- This ensures every existing config has a corresponding system record
INSERT INTO system (uid, id, config, description)
SELECT uuid_generate_v4() as uid,
    CONCAT(
        embedding_config->>'model_family',
        '-',
        embedding_config->>'dimensionality'
    ) as id,
    jsonb_build_object(
        'rag',
        jsonb_build_object('embedding', embedding_config)
    ) as config,
    CONCAT(
        'Auto-migrated system config for ',
        embedding_config->>'model_family',
        ' with ',
        embedding_config->>'dimensionality',
        ' dimensions'
    ) as description
FROM (
        SELECT DISTINCT embedding_config
        FROM knowledge_base
        WHERE embedding_config IS NOT NULL
    ) distinct_configs ON CONFLICT (id) DO NOTHING;
-- Skip if already exists
-- Step 1.8: Add system_uid column to knowledge_base
ALTER TABLE knowledge_base
ADD COLUMN system_uid UUID;
-- Step 1.9: Populate system_uid by matching embedding_config to system records
UPDATE knowledge_base kb
SET system_uid = s.uid
FROM system s
WHERE kb.embedding_config IS NOT NULL
    AND s.config->'rag'->'embedding' = kb.embedding_config;
-- Step 1.9a: For any knowledge_base records without a system_uid (shouldn't happen but handle edge case),
-- assign them to the OpenAI system (default for existing KBs)
UPDATE knowledge_base kb
SET system_uid = (
        SELECT uid
        FROM system
        WHERE id = 'openai'
        LIMIT 1
    )
WHERE system_uid IS NULL;
-- Step 1.10: Make system_uid NOT NULL (all KBs should have a system config)
ALTER TABLE knowledge_base
ALTER COLUMN system_uid
SET NOT NULL;
-- Step 1.11: Add foreign key constraint
ALTER TABLE knowledge_base
ADD CONSTRAINT fk_knowledge_base_system FOREIGN KEY (system_uid) REFERENCES system(uid);
-- Step 1.12: Create index on system_uid for efficient lookups
CREATE INDEX idx_knowledge_base_system_uid ON knowledge_base(system_uid);
-- Step 1.13: Add comment on system_uid column
COMMENT ON COLUMN knowledge_base.system_uid IS 'Foreign key to system table (references system.uid)';
-- Step 1.14: Drop the old embedding_config column
DROP INDEX IF EXISTS idx_knowledge_base_embedding_model_family;
ALTER TABLE knowledge_base DROP COLUMN embedding_config;
-- Step 1.15: Add update_error_message column to store error messages when KB updates fail
ALTER TABLE knowledge_base
ADD COLUMN update_error_message TEXT;
COMMENT ON COLUMN knowledge_base.update_error_message IS 'Error message explaining why the update failed. Populated ONLY when update_status is KNOWLEDGE_BASE_UPDATE_STATUS_FAILED. Empty/NULL for all other statuses (NONE, UPDATING, SYNCING, VALIDATING, SWAPPING, COMPLETED, ROLLED_BACK, ABORTED).';
-- Step 1.16: Add previous_system_uid column to store the previous system UID before update for audit trail
ALTER TABLE knowledge_base
ADD COLUMN previous_system_uid UUID;
COMMENT ON COLUMN knowledge_base.previous_system_uid IS 'Foreign key to system table (references system.uid). Captures the system UID before the update started. Provides historical audit trail showing what system configuration was replaced. Captured when update begins and persists through all status transitions (UPDATING, COMPLETED, FAILED, ROLLED_BACK, ABORTED). NULL for NONE status.';
COMMIT;
