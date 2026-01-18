-- Ensure pgcrypto extension is available for digest function
CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- Migration: Unify System resource to AIP standard
--
-- Current state: System uses RFC-1034 IDs like "openai", "gemini"
-- Target state: System uses hash-based IDs like "sys-a1b2c3d4e5f6g7h8"
--               with display_name for human-readable names
--
-- Changes:
-- 1. Add display_name column (move current id values here)
-- 2. Add slug column (URL-friendly, same as current id values)
-- 3. Convert id to hash-based format with "sys-" prefix
-- 4. Add unique constraint on id
-- ============================================================================
-- Helper function: Generate hash-based ID from UUID
-- ============================================================================
CREATE OR REPLACE FUNCTION generate_system_id(uid UUID) RETURNS TEXT AS $$ BEGIN RETURN 'sys-' || encode(
        substring(
            digest(uid::text, 'sha256')
            from 1 for 8
        ),
        'hex'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- ============================================================================
-- Step 1: Add new columns
-- ============================================================================
ALTER TABLE system
ADD COLUMN IF NOT EXISTS display_name VARCHAR(255);
ALTER TABLE system
ADD COLUMN IF NOT EXISTS slug VARCHAR(255);
-- ============================================================================
-- Step 2: Migrate data - copy current id to display_name and slug
-- ============================================================================
-- Current id values like "openai", "gemini" become the display_name
UPDATE system
SET display_name = INITCAP(id),
    -- "openai" -> "Openai", "gemini" -> "Gemini"
    slug = id -- Keep as URL-friendly slug
WHERE display_name IS NULL;
-- ============================================================================
-- Step 3: Convert id to hash-based format
-- ============================================================================
UPDATE system
SET id = generate_system_id(uid)
WHERE id NOT LIKE 'sys-%';
-- ============================================================================
-- Step 4: Set NOT NULL constraints
-- ============================================================================
ALTER TABLE system
ALTER COLUMN display_name
SET NOT NULL;
-- ============================================================================
-- Step 5: Add unique constraint on id
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_system_id ON system(id)
WHERE delete_time IS NULL;
-- ============================================================================
-- Step 6: Add index for slug lookups
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_system_slug ON system(slug)
WHERE slug IS NOT NULL;
-- ============================================================================
-- Cleanup
-- ============================================================================
DROP FUNCTION IF EXISTS generate_system_id(UUID);
-- ============================================================================
-- Documentation
-- ============================================================================
COMMENT ON COLUMN system.id IS 'Immutable canonical ID with sys- prefix (e.g., sys-a1b2c3d4e5f6g7h8)';
COMMENT ON COLUMN system.display_name IS 'Human-readable name (e.g., OpenAI, Gemini)';
COMMENT ON COLUMN system.slug IS 'URL-friendly identifier (e.g., openai, gemini)';
