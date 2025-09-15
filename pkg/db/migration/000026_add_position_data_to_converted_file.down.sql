BEGIN;

-- Remove position_data column from converted_file table
ALTER TABLE converted_file DROP COLUMN IF EXISTS position_data;

COMMIT;
