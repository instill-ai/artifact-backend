BEGIN;

-- Drop indexes
DROP INDEX IF EXISTS idx_unique_destination;
DROP INDEX IF EXISTS idx_unique_file_uid;

-- Drop the table converted_file
DROP TABLE IF EXISTS converted_file;

COMMIT;
