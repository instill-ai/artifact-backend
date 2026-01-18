-- Down migration: Revert System resource to RFC-1034 format
-- WARNING: This migration loses data - the original RFC-1034 IDs are stored in slug column
-- ============================================================================
-- Step 1: Restore original id from slug
-- ============================================================================
UPDATE system
SET id = slug
WHERE id LIKE 'sys-%'
    AND slug IS NOT NULL;
-- ============================================================================
-- Step 2: Drop indexes
-- ============================================================================
DROP INDEX IF EXISTS idx_system_id;
DROP INDEX IF EXISTS idx_system_slug;
-- ============================================================================
-- Step 3: Drop added columns
-- ============================================================================
ALTER TABLE system DROP COLUMN IF EXISTS display_name;
ALTER TABLE system DROP COLUMN IF EXISTS slug;
ALTER TABLE system DROP COLUMN IF EXISTS aliases;
