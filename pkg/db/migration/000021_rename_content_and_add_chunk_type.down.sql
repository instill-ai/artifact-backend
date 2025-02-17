BEGIN;

-- Drop the new columns
ALTER TABLE text_chunk
DROP COLUMN file_type,
DROP COLUMN content_type;

ALTER TABLE embedding
DROP COLUMN file_type,
DROP COLUMN content_type;

-- Drop the summary column from knowledge_base_file
ALTER TABLE knowledge_base_file
DROP COLUMN summary;

COMMIT;
