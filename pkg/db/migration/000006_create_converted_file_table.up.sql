BEGIN;

-- Create the table converted_file
CREATE TABLE converted_file (
    uid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kb_uid UUID NOT NULL,
    file_uid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(100) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE UNIQUE INDEX idx_unique_destination ON converted_file (destination);
CREATE UNIQUE INDEX idx_unique_file_uid ON converted_file (file_uid);

-- Comments for the table and columns
COMMENT ON TABLE converted_file IS 'Table to store knowledge base files with metadata';
COMMENT ON COLUMN converted_file.uid IS 'Unique identifier for the file';
COMMENT ON COLUMN converted_file.kb_uid IS 'Knowledge base unique identifier, references knowledge base table''s uid field';
COMMENT ON COLUMN converted_file.file_uid IS 'Unique identifier for the origin file in the knowledge base file table';
COMMENT ON COLUMN converted_file.name IS 'Name of the file';
COMMENT ON COLUMN converted_file.type IS 'MIME Type of the file';
COMMENT ON COLUMN converted_file.destination IS 'Destination path of the file';
COMMENT ON COLUMN converted_file.create_time IS 'Timestamp when the record was created';
COMMENT ON COLUMN converted_file.update_time IS 'Timestamp when the record was last updated';

COMMIT;
