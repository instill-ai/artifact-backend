BEGIN;
-- Create the table knowledge_base_files
CREATE TABLE knowledge_base_file (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner UUID NOT NULL,
    kb_uid UUID NOT NULL,
    creator_uid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(100) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    process_status VARCHAR(100) NOT NULL,
    extra_meta_data JSONB,
    content BYTEA,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delete_time TIMESTAMP
);

-- Add the unique index on (kb_uid, name)
CREATE UNIQUE INDEX idx_unique_kb_uid_name_delete_time ON knowledge_base_file (kb_uid, name)
WHERE delete_time IS NULL;

-- Comments for the table and columns
COMMENT ON TABLE knowledge_base_file IS 'Table to store knowledge base files with metadata';
COMMENT ON COLUMN knowledge_base_file.uid IS 'Unique identifier for the file';
COMMENT ON COLUMN knowledge_base_file.create_time IS 'Timestamp when the record was created';
COMMENT ON COLUMN knowledge_base_file.update_time IS 'Timestamp when the record was last updated';
COMMENT ON COLUMN knowledge_base_file.delete_time IS 'Timestamp when the record was deleted (soft delete)';
COMMENT ON COLUMN knowledge_base_file.owner IS 'Owner of the file, references owner table''s uid field';
COMMENT ON COLUMN knowledge_base_file.kb_uid IS 'Knowledge base unique identifier, references knowledge base table''s uid field';
COMMENT ON COLUMN knowledge_base_file.creator_uid IS 'Creator unique identifier, references owner table''s uid field';
COMMENT ON COLUMN knowledge_base_file.name IS 'Name of the file';
COMMENT ON COLUMN knowledge_base_file.process_status IS 'Processing status of the file';
COMMENT ON COLUMN knowledge_base_file.extra_meta_data IS 'Extra metadata in JSON format, e.g., word count, image count';
COMMENT ON COLUMN knowledge_base_file.content IS 'File content stored as byte array';

COMMIT;