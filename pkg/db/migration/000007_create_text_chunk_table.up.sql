BEGIN;
CREATE TABLE text_chunk (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_uid UUID NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    start INT NOT NULL,
    end INT NOT NULL,
    content_dest VARCHAR(255) NOT NULL,
    tokens INT NOT NULL,
    retrievable BOOLEAN NOT NULL DEFAULT true,
    order INT NOT NULL create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
);
-- Create indexes
CREATE UNIQUE INDEX idx_unique_source_table_uid_start_end ON text_chunk (source_table, source_uid, start,end
);
-- Comments for the table and columns
COMMENT ON TABLE text_chunk IS 'Table to store text chunks with metadata';
COMMENT ON COLUMN text_chunk.uid IS 'Unique identifier for the text chunk';
COMMENT ON COLUMN text_chunk.source_uid IS 'Source unique identifier, references source table''s uid field';
COMMENT ON COLUMN text_chunk.source_table IS 'Name of the source table';
COMMENT ON COLUMN text_chunk.start IS 'Start position of the text chunk';
COMMENT ON COLUMN text_chunk.
end IS 'End position of the text chunk';
COMMENT ON COLUMN text_chunk.content_dest IS 'dest of the text chunk''s content in file store';
COMMENT ON COLUMN text_chunk.create_time IS 'Timestamp when the record was created';
COMMENT ON COLUMN text_chunk.update_time IS 'Timestamp when the record was last updated';
COMMIT;
