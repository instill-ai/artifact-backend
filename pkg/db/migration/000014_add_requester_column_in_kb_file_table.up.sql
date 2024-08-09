BEGIN;

-- Add the new column 'requester_uid'
ALTER TABLE knowledge_base_file ADD COLUMN requester_uid UUID;

-- Add a comment for the new column
COMMENT ON COLUMN knowledge_base_file.requester_uid IS 'UID of the requester who initiated the action';

COMMIT;
