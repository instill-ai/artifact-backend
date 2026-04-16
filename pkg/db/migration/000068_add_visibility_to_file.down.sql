DROP INDEX IF EXISTS idx_file_visibility;
ALTER TABLE file DROP COLUMN IF EXISTS visibility;
