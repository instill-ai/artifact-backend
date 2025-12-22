-- Migration 045: Rename owner column to namespace_uid
-- This migration aligns the database schema with the protobuf naming convention.
-- The 'owner' column is renamed to 'namespace_uid' in both knowledge_base and file tables.

BEGIN;

-- Step 1: Rename column in knowledge_base table
ALTER TABLE knowledge_base RENAME COLUMN owner TO namespace_uid;

-- Step 2: Rename column in file table (previously knowledge_base_file, renamed in migration 040)
ALTER TABLE file RENAME COLUMN owner TO namespace_uid;

-- Step 3: Update indexes that reference the old column name (if any)
-- Note: PostgreSQL automatically updates index definitions when columns are renamed,
-- but we may want to rename the indexes themselves for clarity

-- Check and rename index on knowledge_base if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_knowledge_base_owner') THEN
        ALTER INDEX idx_knowledge_base_owner RENAME TO idx_knowledge_base_namespace_uid;
    END IF;
END $$;

-- Check and rename index on file if it exists (from migration 024)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_kb_file_owner_kb_uid_delete_time') THEN
        ALTER INDEX idx_kb_file_owner_kb_uid_delete_time RENAME TO idx_file_namespace_uid_kb_uid_delete_time;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_file_owner_kb_uid_delete_time') THEN
        ALTER INDEX idx_file_owner_kb_uid_delete_time RENAME TO idx_file_namespace_uid_kb_uid_delete_time;
    END IF;
END $$;

-- Step 4: Update comments
COMMENT ON COLUMN knowledge_base.namespace_uid IS 'Namespace UID that owns this knowledge base';
COMMENT ON COLUMN file.namespace_uid IS 'Namespace UID that owns this file';

COMMIT;
