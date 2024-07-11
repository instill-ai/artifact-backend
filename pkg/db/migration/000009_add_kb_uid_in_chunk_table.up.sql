BEGIN;

-- Add the new column kb_uid
ALTER TABLE text_chunk ADD COLUMN kb_uid UUID;

-- Add a comment for the new column
COMMENT ON COLUMN text_chunk.kb_uid IS 'Knowledge Base unique identifier';

-- Create an index on the new column
CREATE INDEX idx_text_chunk_kb_uid ON text_chunk (kb_uid);

COMMIT;
