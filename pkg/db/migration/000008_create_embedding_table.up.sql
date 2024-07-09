BEGIN;
-- Create the table embedding
CREATE TABLE embedding (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_uid UUID NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    vector JSONB NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- Create indexes
CREATE UNIQUE INDEX idx_unique_source ON embedding (source_table, source_uid);
-- Comments for the table and columns
COMMENT ON TABLE embedding IS 'Table to store embeddings with metadata';
COMMENT ON COLUMN embedding.uid IS 'Unique identifier for the embedding';
COMMENT ON COLUMN embedding.source_uid IS 'Source unique identifier, references source table''s uid field';
COMMENT ON COLUMN embedding.source_table IS 'Name of the source table';
COMMENT ON COLUMN embedding.embedding_dest IS 'Destination of the embedding''s content in vector store';
COMMENT ON COLUMN embedding.create_time IS 'Timestamp when the record was created';
COMMENT ON COLUMN embedding.update_time IS 'Timestamp when the record was last updated';
COMMIT;
