BEGIN;

-- Add tags column to knowledge_base_file table
ALTER TABLE knowledge_base_file
  ADD COLUMN tags VARCHAR(255)[];

-- Add comment for the new column
COMMENT ON COLUMN knowledge_base_file.tags IS
  'Array of tags associated with the file. Vector database will use this field for filtering, so indexing here is not necessary.';

COMMIT;
