BEGIN;
-- Revert: Add back original_filename column to converted_file table
ALTER TABLE converted_file
ADD COLUMN original_filename VARCHAR(255) NOT NULL DEFAULT '';
-- Revert: Rename file_uid back to kb_file_uid in text_chunk table
ALTER TABLE text_chunk
    RENAME COLUMN file_uid TO kb_file_uid;
-- Revert comment
COMMENT ON COLUMN text_chunk.kb_file_uid IS 'Knowledge Base file unique identifier';
-- Revert index name
ALTER INDEX idx_text_chunk_file_uid
RENAME TO idx_text_chunk_kb_file_uid;
-- Revert: Rename file_uid back to kb_file_uid in embedding table
ALTER TABLE embedding
    RENAME COLUMN file_uid TO kb_file_uid;
-- Revert comment
COMMENT ON COLUMN embedding.kb_file_uid IS 'Knowledge Base file unique identifier';
-- Revert index name
ALTER INDEX idx_embedding_file_uid
RENAME TO idx_embedding_kb_file_uid;
COMMIT;
