-- Rollback: Remove hash-based ID from Object table and rename display_name back to name
-- Drop indexes
DROP INDEX IF EXISTS idx_object_id;
DROP INDEX IF EXISTS idx_object_display_name;
-- Remove id column
ALTER TABLE object DROP COLUMN IF EXISTS id;
-- Rename display_name back to name
ALTER TABLE object
    RENAME COLUMN display_name TO name;
-- Remove comments
COMMENT ON COLUMN object.name IS NULL;
