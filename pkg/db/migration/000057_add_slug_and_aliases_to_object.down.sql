-- Rollback: Remove slug and aliases from object table
BEGIN;
DROP INDEX IF EXISTS idx_object_slug;
DROP INDEX IF EXISTS idx_object_aliases;
ALTER TABLE object DROP COLUMN IF EXISTS slug;
ALTER TABLE object DROP COLUMN IF EXISTS aliases;
COMMIT;
