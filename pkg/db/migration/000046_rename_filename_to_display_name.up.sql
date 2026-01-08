BEGIN;
-- Rename the filename column to display_name in the file table
-- This aligns with other entities (collection) which use display_name for the human-readable name
ALTER TABLE file
    RENAME COLUMN filename TO display_name;
-- Add id column to file table (defaults to uid for existing rows)
-- This provides a separate URL-safe slug that can differ from uid
ALTER TABLE file
ADD COLUMN IF NOT EXISTS id VARCHAR(255);
-- Set id to uid for existing rows where id is null
UPDATE file
SET id = uid::text
WHERE id IS NULL;
-- Make id not null after backfill
ALTER TABLE file
ALTER COLUMN id
SET NOT NULL;
-- Add description column to file table
ALTER TABLE file
ADD COLUMN IF NOT EXISTS description TEXT;
-- Add display_name column to knowledge_base table
-- This column stores the human-readable display name for UI presentation
ALTER TABLE knowledge_base
ADD COLUMN IF NOT EXISTS display_name VARCHAR(255);
-- Set display_name to id for existing rows where display_name is null
UPDATE knowledge_base
SET display_name = id
WHERE display_name IS NULL;
COMMIT;
