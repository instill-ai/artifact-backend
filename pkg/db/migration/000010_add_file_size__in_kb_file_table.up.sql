BEGIN;

-- Add the new column 'size'
ALTER TABLE knowledge_base_file ADD COLUMN size BIGINT;

-- Add a comment for the new column
COMMENT ON COLUMN knowledge_base_file.size IS 'Size of the file in bytes';


COMMIT;
