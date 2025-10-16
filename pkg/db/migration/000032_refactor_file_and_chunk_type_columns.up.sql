BEGIN;
-- ==========================================
-- PART 1: converted_file table refactoring
-- ==========================================
-- Rename 'name' to 'original_filename' for clarity
ALTER TABLE converted_file
    RENAME COLUMN name TO original_filename;
-- Drop unique constraint on file_uid to allow multiple converted files per file (content + summary)
DROP INDEX IF EXISTS idx_unique_file_uid;
-- Add converted_type column to distinguish content vs summary converted files
ALTER TABLE converted_file
ADD COLUMN converted_type VARCHAR(50) NOT NULL DEFAULT 'content';
-- Populate converted_type for existing records
-- First file = content, second file = summary
WITH ranked_files AS (
    SELECT uid,
        ROW_NUMBER() OVER (
            PARTITION BY file_uid
            ORDER BY create_time ASC
        ) as rn
    FROM converted_file
)
UPDATE converted_file cf
SET converted_type = CASE
        WHEN rf.rn = 1 THEN 'content'
        WHEN rf.rn = 2 THEN 'summary'
        ELSE 'content'
    END
FROM ranked_files rf
WHERE cf.uid = rf.uid;
-- Create index for efficient querying
CREATE INDEX idx_converted_file_file_uid_converted_type ON converted_file(file_uid, converted_type);
-- Rename 'type' to 'content_type' for MIME type clarity
ALTER TABLE converted_file
    RENAME COLUMN type TO content_type;
-- ==========================================
-- PART 2: knowledge_base_file table refactoring
-- ==========================================
-- Drop redundant content and summary columns (now stored as converted_files)
ALTER TABLE knowledge_base_file DROP COLUMN IF EXISTS content;
ALTER TABLE knowledge_base_file DROP COLUMN IF EXISTS summary;
-- Rename 'type' to 'file_type' (stores FileType enum string like "FILE_TYPE_PDF")
ALTER TABLE knowledge_base_file
    RENAME COLUMN type TO file_type;
-- ==========================================
-- PART 3: text_chunk table refactoring
-- ==========================================
-- Rename 'content_type' to 'chunk_type' (type of chunk: content/summary/augmented)
ALTER TABLE text_chunk
    RENAME COLUMN content_type TO chunk_type;
-- Rename 'file_type' to 'content_type' for MIME type clarity
ALTER TABLE text_chunk
    RENAME COLUMN file_type TO content_type;
-- Update chunk_type values from "chunk" to "content" (aligns with protobuf enum TYPE_CONTENT)
UPDATE text_chunk
SET chunk_type = 'content'
WHERE chunk_type = 'chunk';
-- ==========================================
-- PART 4: embedding table refactoring
-- ==========================================
-- Rename 'content_type' to 'chunk_type' (type of chunk: content/summary/augmented)
ALTER TABLE embedding
    RENAME COLUMN content_type TO chunk_type;
-- Rename 'file_type' to 'content_type' for MIME type clarity
ALTER TABLE embedding
    RENAME COLUMN file_type TO content_type;
-- Update chunk_type values from "chunk" to "content" (aligns with protobuf enum TYPE_CONTENT)
UPDATE embedding
SET chunk_type = 'content'
WHERE chunk_type = 'chunk';
COMMIT;
