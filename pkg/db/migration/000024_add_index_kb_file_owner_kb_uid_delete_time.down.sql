BEGIN;

-- Drop the index for owner, kb_uid and delete_time
DROP INDEX IF EXISTS idx_kb_file_owner_kb_uid_delete_time;

-- Drop the index for process_status and delete_time
DROP INDEX IF EXISTS idx_kb_file_process_status_delete_time;

COMMIT;
