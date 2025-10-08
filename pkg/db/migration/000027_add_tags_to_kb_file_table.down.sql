BEGIN;
ALTER TABLE knowledge_base_file
DROP COLUMN tags;
COMMIT;
