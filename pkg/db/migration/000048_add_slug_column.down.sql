-- Rollback: Remove slug column

-- Drop indexes first
DROP INDEX IF EXISTS idx_knowledge_base_slug;
DROP INDEX IF EXISTS idx_file_slug;

-- Remove slug columns
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS slug;
ALTER TABLE file DROP COLUMN IF EXISTS slug;
