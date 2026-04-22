DROP INDEX IF EXISTS idx_kb_entity_aliases;

ALTER TABLE kb_entity
    DROP COLUMN IF EXISTS aliases;
