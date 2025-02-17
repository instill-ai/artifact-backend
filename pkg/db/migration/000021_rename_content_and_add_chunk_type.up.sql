BEGIN;

-- Add new columns file_type and content_type for text_chunk
ALTER TABLE text_chunk
ADD COLUMN file_type VARCHAR(255) NOT NULL,
ADD COLUMN content_type VARCHAR(255) NOT NULL;

-- Add a comment for the new column
COMMENT ON COLUMN text_chunk.file_type IS 'file type';
COMMENT ON COLUMN text_chunk.content_type IS 'content type';

-- Add new columns file_type and content_type for text_chunk
ALTER TABLE embedding
ADD COLUMN file_type VARCHAR(255) NOT NULL,
ADD COLUMN content_type VARCHAR(255) NOT NULL;

-- Add a comment for the new column
COMMENT ON COLUMN embedding.file_type IS 'file type';
COMMENT ON COLUMN embedding.content_type IS 'content type';

-- Add new columns summary for knowledge_base_file
ALTER TABLE knowledge_base_file ADD COLUMN summary BYTEA;

COMMIT;
