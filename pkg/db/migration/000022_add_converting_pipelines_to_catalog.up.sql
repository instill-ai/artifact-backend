BEGIN;

ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS converting_pipelines VARCHAR(255)[];

COMMIT;
