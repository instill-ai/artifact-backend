BEGIN;
-- Rename kb_file_uid to file_uid in text_chunk table
ALTER TABLE text_chunk
    RENAME COLUMN kb_file_uid TO file_uid;
-- Update comment for the renamed column
COMMENT ON COLUMN text_chunk.file_uid IS 'Knowledge Base file unique identifier (foreign key to knowledge_base_file.uid)';
-- Rename the index to match new column name
ALTER INDEX idx_text_chunk_kb_file_uid
RENAME TO idx_text_chunk_file_uid;
-- Rename kb_file_uid to file_uid in embedding table
ALTER TABLE embedding
    RENAME COLUMN kb_file_uid TO file_uid;
-- Update comment for the renamed column
COMMENT ON COLUMN embedding.file_uid IS 'Knowledge Base file unique identifier (foreign key to knowledge_base_file.uid)';
-- Rename the index to match new column name
ALTER INDEX idx_embedding_kb_file_uid
RENAME TO idx_embedding_file_uid;
-- Drop redundant original_filename column from converted_file table
-- The filename can be retrieved from knowledge_base_file table via file_uid foreign key
ALTER TABLE converted_file DROP COLUMN original_filename;
COMMIT;
