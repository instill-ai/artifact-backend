BEGIN;
-- Revert: Remove the unique constraint and restore the non-unique index
DROP INDEX IF EXISTS idx_unique_converted_file_file_uid_type;
-- Restore the original non-unique index
CREATE INDEX idx_converted_file_file_uid_converted_type ON converted_file (file_uid, converted_type);
COMMIT;
