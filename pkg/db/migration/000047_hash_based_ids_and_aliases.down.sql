-- Rollback: Hash-based IDs and Aliases
-- WARNING: This rollback will revert IDs to their aliases (old IDs)
-- If aliases are empty, the hash suffix will be stripped

-- Drop indexes
DROP INDEX IF EXISTS idx_knowledge_base_aliases;
DROP INDEX IF EXISTS idx_file_aliases;

-- Revert knowledge_base IDs from hash-based to aliases (if available) or strip hash suffix
UPDATE knowledge_base
SET id = COALESCE(aliases[1], REGEXP_REPLACE(id, '-[a-f0-9]{8}$', ''));

-- Revert file IDs from hash-based to aliases (if available) or uid::text
UPDATE file
SET id = COALESCE(aliases[1], uid::text);

-- Remove aliases columns
ALTER TABLE file DROP COLUMN IF EXISTS aliases;
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS aliases;
