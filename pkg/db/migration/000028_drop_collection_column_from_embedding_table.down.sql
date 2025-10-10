BEGIN;
-- Re-add the collection column to the embedding table
ALTER TABLE embedding ADD COLUMN collection VARCHAR(255) NOT NULL DEFAULT '';
COMMENT ON COLUMN embedding.collection IS 'Destination of the embedding''s content in vector store';
COMMIT;
