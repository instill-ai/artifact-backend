BEGIN;

-- Remove all CHECK constraints on enum fields

-- TABLE: file
ALTER TABLE file
    DROP CONSTRAINT IF EXISTS check_file_type_format;

ALTER TABLE file
    DROP CONSTRAINT IF EXISTS check_process_status_format;

-- TABLE: knowledge_base
ALTER TABLE knowledge_base
    DROP CONSTRAINT IF EXISTS check_update_status_format;

-- TABLE: converted_file
ALTER TABLE converted_file
    DROP CONSTRAINT IF EXISTS check_converted_type_format;

-- TABLE: chunk
ALTER TABLE chunk
    DROP CONSTRAINT IF EXISTS check_chunk_type_format;

-- TABLE: embedding
ALTER TABLE embedding
    DROP CONSTRAINT IF EXISTS check_embedding_chunk_type_format;

COMMIT;
