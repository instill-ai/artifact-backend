BEGIN;

-- Reverse migration: Rename storage_path back to content_dest
ALTER TABLE chunk RENAME COLUMN storage_path TO content_dest;

COMMIT;
