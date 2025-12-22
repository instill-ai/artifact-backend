-- Migration 045 Rollback: Revert namespace_uid column back to owner

BEGIN;

-- Step 1: Rename column back in knowledge_base table
ALTER TABLE knowledge_base RENAME COLUMN namespace_uid TO owner;

-- Step 2: Rename column back in file table
ALTER TABLE file RENAME COLUMN namespace_uid TO owner;

-- Step 3: Revert index names
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_knowledge_base_namespace_uid') THEN
        ALTER INDEX idx_knowledge_base_namespace_uid RENAME TO idx_knowledge_base_owner;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_file_namespace_uid_kb_uid_delete_time') THEN
        ALTER INDEX idx_file_namespace_uid_kb_uid_delete_time RENAME TO idx_file_owner_kb_uid_delete_time;
    END IF;
END $$;

-- Step 4: Revert comments
COMMENT ON COLUMN knowledge_base.owner IS 'Owner of the knowledge base, references owner table''s uid field';
COMMENT ON COLUMN file.owner IS 'Owner of the file, references owner table''s uid field';

COMMIT;
