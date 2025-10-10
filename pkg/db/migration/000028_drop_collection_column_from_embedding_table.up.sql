BEGIN;
-- Drop the collection column from the embedding table since it can be derived from kb_uid
ALTER TABLE embedding DROP COLUMN collection;
COMMIT;
