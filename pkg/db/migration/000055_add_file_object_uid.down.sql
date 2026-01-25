-- Remove object_uid column and index from file table
DROP INDEX IF EXISTS idx_file_object_uid;
ALTER TABLE file DROP COLUMN IF EXISTS object_uid;
