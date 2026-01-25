-- Add object_uid foreign key column to file table
-- This establishes a proper relationship between files and their blob storage objects
ALTER TABLE file ADD COLUMN object_uid UUID REFERENCES object(uid) ON DELETE SET NULL;

-- Create index for efficient joins when looking up object information for files
CREATE INDEX idx_file_object_uid ON file(object_uid);

-- Backfill existing files: match files to objects by storage_path
-- Both file.storage_path and object.storage_path use the same format
UPDATE file f
SET object_uid = o.uid
FROM object o
WHERE f.storage_path IS NOT NULL
  AND f.storage_path != ''
  AND o.storage_path = f.storage_path;
