BEGIN;
-- Add the new columns kb_uid and kb_file_uid
ALTER TABLE embedding
    ADD COLUMN kb_uid UUID,
    ADD COLUMN kb_file_uid UUID;

-- Add comments for the new columns
COMMENT ON COLUMN embedding.kb_uid IS 'Knowledge Base unique identifier';
COMMENT ON COLUMN embedding.kb_file_uid IS 'Knowledge Base file unique identifier';

-- Create indexes on the new columns
CREATE INDEX idx_embedding_kb_uid ON embedding (kb_uid);
CREATE INDEX idx_embedding_kb_file_uid ON embedding (kb_file_uid);
COMMIT;
