BEGIN;

-- Add the new column kb_file_uid
ALTER TABLE text_chunk ADD COLUMN kb_file_uid UUID;

-- Add a comment for the new column
COMMENT ON COLUMN text_chunk.kb_file_uid IS 'Knowledge Base file unique identifier';

-- Create an index on the new column
CREATE INDEX idx_text_chunk_kb_file_uid ON text_chunk (kb_file_uid);

COMMIT;
