BEGIN;
-- Rename catalog_type column to knowledge_base_type
ALTER TABLE knowledge_base
    RENAME COLUMN catalog_type TO knowledge_base_type;
-- Update enum values from CATALOG_TYPE_* to KNOWLEDGE_BASE_TYPE_*
UPDATE knowledge_base
SET knowledge_base_type = 'KNOWLEDGE_BASE_TYPE_PERSISTENT'
WHERE knowledge_base_type = 'CATALOG_TYPE_PERSISTENT';
UPDATE knowledge_base
SET knowledge_base_type = 'KNOWLEDGE_BASE_TYPE_EPHEMERAL'
WHERE knowledge_base_type = 'CATALOG_TYPE_EPHEMERAL';
UPDATE knowledge_base
SET knowledge_base_type = 'KNOWLEDGE_BASE_TYPE_UNSPECIFIED'
WHERE knowledge_base_type = 'CATALOG_TYPE_UNSPECIFIED';
-- Update column comment
COMMENT ON COLUMN knowledge_base.knowledge_base_type IS 'Type of the knowledge base (stored as protobuf enum string, e.g., KNOWLEDGE_BASE_TYPE_PERSISTENT)';
-- Rename knowledge_base_file → file
ALTER TABLE knowledge_base_file
    RENAME TO file;
-- Rename text_chunk → chunk
ALTER TABLE text_chunk
    RENAME TO chunk;
-- Update index names for file table
ALTER INDEX IF EXISTS idx_unique_kb_uid_filename_delete_time
RENAME TO idx_file_unique_kb_uid_filename_delete_time;
ALTER INDEX IF EXISTS idx_kb_file_owner_kb_uid_delete_time
RENAME TO idx_file_owner_kb_uid_delete_time;
-- Update index names for chunk table
ALTER INDEX IF EXISTS idx_unique_source_table_uid_start_end
RENAME TO idx_chunk_unique_source_table_uid_start_end;
-- Update table comments
COMMENT ON TABLE file IS 'Table to store knowledge base files with metadata';
COMMENT ON TABLE chunk IS 'Table to store text chunks with metadata';
COMMIT;
