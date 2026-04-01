BEGIN;
ALTER TABLE knowledge_base ALTER COLUMN usage TYPE integer;
COMMIT;
