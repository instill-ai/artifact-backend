BEGIN;

-- Reverse migration: Unify Object Schema & Rename storage_path back to destination
-- Note: This drops the new columns but preserves original data in name/destination

-- ============================================================================
-- Revert file and converted_file table renames
-- ============================================================================
ALTER TABLE file RENAME COLUMN storage_path TO destination;
ALTER TABLE converted_file RENAME COLUMN storage_path TO destination;

-- ============================================================================
-- Revert object table changes
-- ============================================================================
-- Drop unique constraint
DROP INDEX IF EXISTS idx_object_namespace_id;

-- Drop new columns
ALTER TABLE object DROP COLUMN IF EXISTS id;
ALTER TABLE object DROP COLUMN IF EXISTS display_name;
ALTER TABLE object DROP COLUMN IF EXISTS storage_path;

COMMIT;
