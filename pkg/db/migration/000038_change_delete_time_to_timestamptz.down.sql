-- Rollback migration 000038: Change delete_time back from TIMESTAMPTZ to TIMESTAMP
BEGIN;
-- Knowledge base table
ALTER TABLE knowledge_base
ALTER COLUMN delete_time TYPE TIMESTAMP;
-- Knowledge base file table
ALTER TABLE knowledge_base_file
ALTER COLUMN delete_time TYPE TIMESTAMP;
-- System table
ALTER TABLE system
ALTER COLUMN delete_time TYPE TIMESTAMP;
-- Object table
ALTER TABLE object
ALTER COLUMN delete_time TYPE TIMESTAMP;
-- Object URL table
ALTER TABLE object_url
ALTER COLUMN delete_time TYPE TIMESTAMP;
-- Conversation table (if exists)
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'conversation'
        AND column_name = 'delete_time'
) THEN
ALTER TABLE conversation
ALTER COLUMN delete_time TYPE TIMESTAMP;
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
ALTER COLUMN delete_time TYPE TIMESTAMP;
END IF;
END $$;
COMMIT;
