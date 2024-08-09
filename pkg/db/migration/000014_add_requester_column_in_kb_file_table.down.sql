BEGIN;
-- Remove the 'requester_uid' column
ALTER TABLE knowledge_base_file DROP COLUMN requester_uid;
COMMIT;
