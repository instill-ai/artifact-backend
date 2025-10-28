BEGIN;
-- Revert table renames
ALTER TABLE file
    RENAME TO knowledge_base_file;
ALTER TABLE chunk
    RENAME TO text_chunk;
-- Restore old index names
ALTER INDEX IF EXISTS idx_file_unique_kb_uid_filename_delete_time
RENAME TO idx_unique_kb_uid_filename_delete_time;
ALTER INDEX IF EXISTS idx_file_owner_kb_uid_delete_time
RENAME TO idx_kb_file_owner_kb_uid_delete_time;
ALTER INDEX IF EXISTS idx_chunk_unique_source_table_uid_start_end
RENAME TO idx_unique_source_table_uid_start_end;
-- Restore old table comments
COMMENT ON TABLE knowledge_base_file IS 'Table to store knowledge base files with metadata';
COMMENT ON TABLE text_chunk IS 'Table to store text chunks with metadata';
-- Revert enum values from KNOWLEDGE_BASE_TYPE_* back to CATALOG_TYPE_*
UPDATE knowledge_base
SET catalog_type = 'CATALOG_TYPE_PERSISTENT'
WHERE catalog_type = 'KNOWLEDGE_BASE_TYPE_PERSISTENT';
UPDATE knowledge_base
SET catalog_type = 'CATALOG_TYPE_EPHEMERAL'
WHERE catalog_type = 'KNOWLEDGE_BASE_TYPE_EPHEMERAL';
UPDATE knowledge_base
SET catalog_type = 'CATALOG_TYPE_UNSPECIFIED'
WHERE catalog_type = 'KNOWLEDGE_BASE_TYPE_UNSPECIFIED';
-- Rename knowledge_base_type column back to catalog_type
ALTER TABLE knowledge_base
    RENAME COLUMN knowledge_base_type TO catalog_type;
-- Restore original column comment
COMMENT ON COLUMN knowledge_base.catalog_type IS 'Type of the knowledge base catalog (stored as protobuf enum string, e.g., CATALOG_TYPE_PERSISTENT)';
COMMIT;
