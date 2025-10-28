BEGIN;
-- 1. Rename name to filename in knowledge_base_file table
ALTER TABLE knowledge_base_file
    RENAME COLUMN name TO filename;
-- Update comment for the renamed column
COMMENT ON COLUMN knowledge_base_file.filename IS 'Filename of the uploaded file';
-- Note: The unique index idx_unique_kb_uid_name_delete_time was already dropped in migration 000023
-- No index rename needed
-- 2. Drop redundant 'name' column from knowledge_base table
-- The Google AIP resource 'name' (format: namespaces/{ns}/catalogs/{id})
-- is now computed dynamically from owner and kb_id, not stored in the database
ALTER TABLE knowledge_base DROP COLUMN IF EXISTS name;
COMMIT;
