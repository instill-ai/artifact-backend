BEGIN;
-- Revert: Rename display_name back to filename
ALTER TABLE file
    RENAME COLUMN display_name TO filename;
-- Remove id column from file table
ALTER TABLE file DROP COLUMN IF EXISTS id;
-- Remove description column from file table
ALTER TABLE file DROP COLUMN IF EXISTS description;
-- Remove display_name column from knowledge_base table
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS display_name;
COMMIT;
