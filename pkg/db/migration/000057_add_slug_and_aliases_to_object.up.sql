-- Migration: Add slug and aliases to object table
-- This migration adds the missing slug and aliases columns to the object table
-- which were intended to be added in migration 000051 but were missed if that
-- migration was already run before it was updated.
BEGIN;
-- Step 1: Add missing columns
ALTER TABLE object
ADD COLUMN IF NOT EXISTS slug VARCHAR(255);
ALTER TABLE object
ADD COLUMN IF NOT EXISTS aliases TEXT [] DEFAULT '{}';
-- Step 2: Backfill data for slug
-- Generate slug from display_name (URL-safe: lowercase, alphanumeric with hyphens)
UPDATE object
SET slug = LOWER(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    COALESCE(display_name, ''),
                    '[^a-zA-Z0-9\s-]',
                    '',
                    'g'
                ),
                '\s+',
                '-',
                'g'
            ),
            '-+',
            '-',
            'g'
        )
    )
WHERE slug IS NULL;
-- Step 3: Add indexes for slug and aliases lookups
CREATE INDEX IF NOT EXISTS idx_object_slug ON object(slug)
WHERE slug IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_object_aliases ON object USING GIN(aliases);
-- Step 4: Add comments for documentation
COMMENT ON COLUMN object.slug IS 'URL-friendly identifier derived from display_name';
COMMENT ON COLUMN object.aliases IS 'Previous slugs for backward compatibility when display_name changes';
COMMIT;
