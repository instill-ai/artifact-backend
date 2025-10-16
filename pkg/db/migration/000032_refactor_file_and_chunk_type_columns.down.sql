-- ==========================================
-- Revert: File and chunk type column refactoring
-- ==========================================
BEGIN;
-- ==========================================
-- PART 4: Revert embedding table changes (reverse order)
-- ==========================================
-- Revert: Update chunk_type values from "content" back to "chunk"
UPDATE embedding
SET chunk_type = 'chunk'
WHERE chunk_type = 'content';
-- Revert: Rename content_type back to file_type
ALTER TABLE embedding
    RENAME COLUMN content_type TO file_type;
-- Revert: Rename chunk_type back to content_type
ALTER TABLE embedding
    RENAME COLUMN chunk_type TO content_type;
-- ==========================================
-- PART 3: Revert text_chunk table changes
-- ==========================================
-- Revert: Update chunk_type values from "content" back to "chunk"
UPDATE text_chunk
SET chunk_type = 'chunk'
WHERE chunk_type = 'content';
-- Revert: Rename content_type back to file_type
ALTER TABLE text_chunk
    RENAME COLUMN content_type TO file_type;
-- Revert: Rename chunk_type back to content_type
ALTER TABLE text_chunk
    RENAME COLUMN chunk_type TO content_type;
-- ==========================================
-- PART 2: Revert knowledge_base_file table changes
-- ==========================================
-- Revert: Rename file_type back to type
ALTER TABLE knowledge_base_file
    RENAME COLUMN file_type TO type;
-- Restore content and summary columns
ALTER TABLE knowledge_base_file
ADD COLUMN IF NOT EXISTS content bytea;
ALTER TABLE knowledge_base_file
ADD COLUMN IF NOT EXISTS summary bytea;
-- ==========================================
-- PART 1: Revert converted_file table changes
-- ==========================================
-- Revert: Rename content_type back to type
ALTER TABLE converted_file
    RENAME COLUMN content_type TO type;
-- Drop the index
DROP INDEX IF EXISTS idx_converted_file_file_uid_converted_type;
-- Remove converted_type column
ALTER TABLE converted_file DROP COLUMN IF EXISTS converted_type;
-- Restore unique constraint (may fail if multiple files exist)
CREATE UNIQUE INDEX idx_unique_file_uid ON converted_file (file_uid);
-- Revert: Rename original_filename back to name
ALTER TABLE converted_file
    RENAME COLUMN original_filename TO name;
COMMIT;
