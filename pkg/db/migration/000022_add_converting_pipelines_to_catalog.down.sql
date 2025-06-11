BEGIN;

ALTER TABLE knowledge_base DROP COLUMN IF EXISTS converting_pipelines;

COMMIT;
