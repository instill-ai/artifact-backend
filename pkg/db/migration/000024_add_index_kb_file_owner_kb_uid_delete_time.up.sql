BEGIN;

-- Add index to optimize queries filtering by owner, kb_uid and checking delete_time is not null
CREATE INDEX IF NOT EXISTS idx_kb_file_owner_kb_uid_delete_time
    ON knowledge_base_file (owner, kb_uid)
    WHERE delete_time IS NULL;

-- Add index to optimize queries filtering by process_status and checking delete_time is not null
-- This is used by GetNeedProcessFiles and other worker processes
CREATE INDEX IF NOT EXISTS idx_kb_file_process_status_delete_time
    ON knowledge_base_file (process_status)
    WHERE delete_time IS NULL;

COMMIT;
