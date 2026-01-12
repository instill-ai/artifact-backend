-- Migration: Hash-based IDs and Aliases
-- This migration adds aliases columns (for future runtime renames) and converts existing IDs to hash-based format
-- Format: {slug}-{sha256(uid)[:8]}
-- Note: Old IDs are NOT preserved in aliases (alpha breaking change). Aliases column is for future renames only.

-- Add aliases columns (for future runtime display_name changes)
ALTER TABLE knowledge_base ADD COLUMN IF NOT EXISTS aliases TEXT[] DEFAULT '{}';
ALTER TABLE file ADD COLUMN IF NOT EXISTS aliases TEXT[] DEFAULT '{}';

-- Convert existing knowledge_base IDs to hash-based format (no backward compatibility)
UPDATE knowledge_base
SET id = id || '-' || SUBSTRING(encode(sha256(uid::text::bytea), 'hex'), 1, 8)
WHERE id NOT LIKE '%-%-%-%';

-- Convert existing file IDs to hash-based format (no backward compatibility)
-- Current id is uid::text (from migration 000046), convert to slug-based hash ID from display_name
UPDATE file
SET id = LOWER(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(display_name, '[^a-zA-Z0-9\s-]', '', 'g'),
            '\s+', '-', 'g'
        ),
        '-+', '-', 'g'
    )
) || '-' || SUBSTRING(encode(sha256(uid::text::bytea), 'hex'), 1, 8)
WHERE id NOT LIKE '%-%-%-%';

-- Create indexes for alias lookups (for future runtime renames)
CREATE INDEX IF NOT EXISTS idx_knowledge_base_aliases ON knowledge_base USING GIN(aliases);
CREATE INDEX IF NOT EXISTS idx_file_aliases ON file USING GIN(aliases);
