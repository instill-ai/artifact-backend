-- Ensure pgcrypto extension is available for digest function
CREATE EXTENSION IF NOT EXISTS pgcrypto;

BEGIN;

-- ============================================================================
-- Migration: Unify Object Schema & Rename destination to storage_path
-- ============================================================================
-- This migration:
-- 1. Unifies the object table to follow the AIP resource pattern:
--    - Add 'id' column with hash-based identifier (obj-{hash})
--    - Rename 'name' to 'display_name' (user-provided filename)
--    - Rename 'destination' to 'storage_path' (clearer semantics)
--    - Add unique constraint on (namespace_uid, id)
-- 2. Renames 'destination' to 'storage_path' in file and converted_file tables
--    for consistency across all storage-related tables
-- ============================================================================

-- Helper function to generate hash-based ID from UID
CREATE OR REPLACE FUNCTION generate_object_id(uid UUID)
RETURNS TEXT AS $$
BEGIN
    RETURN 'obj-' || encode(substring(digest(uid::text, 'sha256') from 1 for 8), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Step 1: Add new columns
ALTER TABLE object ADD COLUMN IF NOT EXISTS id VARCHAR(255);
ALTER TABLE object ADD COLUMN IF NOT EXISTS display_name VARCHAR(1040);
ALTER TABLE object ADD COLUMN IF NOT EXISTS storage_path VARCHAR(255);

-- Step 2: Backfill data
-- - Generate hash-based id from uid
-- - Copy name to display_name
-- - Copy destination to storage_path
UPDATE object
SET
    id = generate_object_id(uid),
    display_name = name,
    storage_path = destination
WHERE id IS NULL;

-- Step 3: Set NOT NULL constraints
ALTER TABLE object ALTER COLUMN id SET NOT NULL;
-- display_name can be NULL (optional)
-- storage_path can be NULL initially (set after upload)

-- Step 4: Add unique constraint on id within namespace (with soft-delete support)
CREATE UNIQUE INDEX IF NOT EXISTS idx_object_namespace_id
    ON object(namespace_uid, id) WHERE delete_time IS NULL;

-- Step 5: Drop old columns (after verifying data migration)
-- Note: Keep old columns for backward compatibility during transition
-- These can be dropped in a future migration after code is updated:
-- ALTER TABLE object DROP COLUMN IF EXISTS name;
-- ALTER TABLE object DROP COLUMN IF EXISTS destination;

-- Step 6: Add comments for documentation
COMMENT ON COLUMN object.id IS 'Hash-based canonical resource ID (obj-{hash}), unique within namespace';
COMMENT ON COLUMN object.display_name IS 'User-provided filename for display purposes';
COMMENT ON COLUMN object.storage_path IS 'Path to the object in blob storage (e.g., ns-{nsUID}/obj-{objUID})';

-- Cleanup helper function
DROP FUNCTION IF EXISTS generate_object_id(UUID);

-- ============================================================================
-- Rename destination to storage_path in file and converted_file tables
-- ============================================================================
-- This aligns file and converted_file tables with the object table naming

ALTER TABLE file RENAME COLUMN destination TO storage_path;
ALTER TABLE converted_file RENAME COLUMN destination TO storage_path;

COMMENT ON COLUMN file.storage_path IS 'Path to the file in blob storage';
COMMENT ON COLUMN converted_file.storage_path IS 'Path to the converted file in blob storage';

COMMIT;
