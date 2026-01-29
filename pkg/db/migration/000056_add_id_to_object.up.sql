-- Migration: Add hash-based ID to Object table and rename name to display_name
--
-- Current state: Object uses only uid (UUID) with name column for filename
-- Target state: Object uses hash-based id like "obj-a1b2c3d4e5f6g7h8"
--               with display_name for human-readable names (user-provided filename)
--
-- Changes:
-- 1. Rename name column to display_name for AIP standard consistency
-- 2. Add id column with hash-based format using "obj-" prefix
-- 3. Add unique constraint on id within namespace
-- ============================================================================
-- Helper function: Generate hash-based ID from UUID
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE OR REPLACE FUNCTION generate_object_id(uid UUID) RETURNS TEXT AS $$ BEGIN RETURN 'obj-' || encode(
        substring(
            digest(uid::text, 'sha256')
            from 1 for 8
        ),
        'hex'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- ============================================================================
-- Step 1: Rename name column to display_name or drop if already exists
-- ============================================================================
DO $$ BEGIN IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'object'
        AND column_name = 'name'
) THEN IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'object'
        AND column_name = 'display_name'
) THEN -- Both exist, just drop name as display_name was already added by a previous migration (e.g., 000051)
ALTER TABLE object DROP COLUMN name;
ELSE -- Only name exists, rename it
ALTER TABLE object
    RENAME COLUMN name TO display_name;
END IF;
END IF;
END $$;
-- ============================================================================
-- Step 2: Add id column
-- ============================================================================
ALTER TABLE object
ADD COLUMN IF NOT EXISTS id VARCHAR(255);
-- ============================================================================
-- Step 3: Generate hash-based IDs for existing records
-- ============================================================================
UPDATE object
SET id = generate_object_id(uid)
WHERE id IS NULL;
-- ============================================================================
-- Step 4: Set NOT NULL constraint
-- ============================================================================
ALTER TABLE object
ALTER COLUMN id
SET NOT NULL;
-- ============================================================================
-- Step 5: Add unique constraint on id within namespace
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_object_id ON object(id)
WHERE delete_time IS NULL;
-- ============================================================================
-- Step 6: Add index for display_name lookups (performance optimization)
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_object_display_name ON object(display_name)
WHERE display_name IS NOT NULL;
-- ============================================================================
-- Cleanup
-- ============================================================================
DROP FUNCTION IF EXISTS generate_object_id(UUID);
-- ============================================================================
-- Documentation
-- ============================================================================
COMMENT ON COLUMN object.id IS 'Immutable canonical ID with obj- prefix (e.g., obj-a1b2c3d4e5f6g7h8)';
COMMENT ON COLUMN object.display_name IS 'Human-readable filename provided by user';
