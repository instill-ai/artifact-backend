-- Migration: Hash-based IDs and Aliases
-- This migration adds aliases columns (for future runtime renames) and converts existing IDs to hash-based format
-- Format: {prefix}-{sha256(uid)[:8]} where prefix is 'kb' for knowledge_base and 'file' for file
-- Note: Old IDs are NOT preserved in aliases (alpha breaking change). Aliases column is for future renames only.
-- Add aliases columns (for future runtime display_name changes)
ALTER TABLE knowledge_base
ADD COLUMN IF NOT EXISTS aliases TEXT [] DEFAULT '{}';
ALTER TABLE file
ADD COLUMN IF NOT EXISTS aliases TEXT [] DEFAULT '{}';
-- Convert existing knowledge_base IDs to hash-based format (no backward compatibility)
-- Format: kb-{hash} (e.g., kb-50edd502)
UPDATE knowledge_base
SET id = 'kb-' || SUBSTRING(encode(sha256(uid::text::bytea), 'hex'), 1, 8)
WHERE id NOT LIKE 'kb-%';
-- Convert existing file IDs to hash-based format (no backward compatibility)
-- Format: file-{hash} (e.g., file-a1b2c3d4)
UPDATE file
SET id = 'file-' || SUBSTRING(encode(sha256(uid::text::bytea), 'hex'), 1, 8)
WHERE id NOT LIKE 'file-%';
-- Create indexes for alias lookups (for future runtime renames)
CREATE INDEX IF NOT EXISTS idx_knowledge_base_aliases ON knowledge_base USING GIN(aliases);
CREATE INDEX IF NOT EXISTS idx_file_aliases ON file USING GIN(aliases);
