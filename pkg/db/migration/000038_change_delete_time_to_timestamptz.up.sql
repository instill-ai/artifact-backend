-- Migration 000038: Change delete_time from TIMESTAMP to TIMESTAMPTZ
-- This aligns with pipeline-backend and model-backend which use gorm.DeletedAt
BEGIN;
-- Knowledge base table
ALTER TABLE knowledge_base
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
-- Knowledge base file table
ALTER TABLE knowledge_base_file
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
-- System table
ALTER TABLE system
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
-- Object table
ALTER TABLE object
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
-- Object URL table
ALTER TABLE object_url
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
-- Conversation table (if exists)
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'conversation'
        AND column_name = 'delete_time'
) THEN
ALTER TABLE conversation
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
END IF;
END $$;
-- Message table (if exists)
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'message'
        AND column_name = 'delete_time'
) THEN
ALTER TABLE message
ALTER COLUMN delete_time TYPE TIMESTAMPTZ USING delete_time AT TIME ZONE 'UTC';
END IF;
END $$;
COMMIT;
