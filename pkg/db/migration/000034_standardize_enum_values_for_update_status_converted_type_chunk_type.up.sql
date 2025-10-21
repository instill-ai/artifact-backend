BEGIN;
-- ==========================================
-- PART 1: Standardize converted_file.converted_type enum values
-- ==========================================
-- Update legacy converted_type values to use full enum names for consistency
-- This ensures all records use the protobuf enum string representation
UPDATE converted_file
SET converted_type = 'CONVERTED_FILE_TYPE_CONTENT'
WHERE converted_type = 'content';
UPDATE converted_file
SET converted_type = 'CONVERTED_FILE_TYPE_SUMMARY'
WHERE converted_type = 'summary';
-- Add comment explaining the format
COMMENT ON COLUMN converted_file.converted_type IS 'Type of the converted file (stored as protobuf enum string, e.g., CONVERTED_FILE_TYPE_CONTENT, CONVERTED_FILE_TYPE_SUMMARY)';
-- ==========================================
-- PART 2: Standardize text_chunk.chunk_type enum values
-- ==========================================
-- Update legacy chunk_type values to use full enum names for consistency
UPDATE text_chunk
SET chunk_type = 'TYPE_CONTENT'
WHERE chunk_type = 'content';
UPDATE text_chunk
SET chunk_type = 'TYPE_SUMMARY'
WHERE chunk_type = 'summary';
UPDATE text_chunk
SET chunk_type = 'TYPE_AUGMENTED'
WHERE chunk_type = 'augmented';
-- Add comment explaining the format
COMMENT ON COLUMN text_chunk.chunk_type IS 'Type of the chunk (stored as protobuf enum string, e.g., TYPE_CONTENT, TYPE_SUMMARY, TYPE_AUGMENTED)';
-- ==========================================
-- PART 3: Standardize knowledge_base.update_status enum values
-- ==========================================
-- Update legacy update_status values to use full enum names for consistency
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING'
WHERE update_status = 'updating';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING'
WHERE update_status = 'syncing';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING'
WHERE update_status = 'validating';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING'
WHERE update_status = 'swapping';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED'
WHERE update_status = 'completed';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_FAILED'
WHERE update_status = 'failed';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK'
WHERE update_status = 'rolled_back';
UPDATE knowledge_base
SET update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED'
WHERE update_status = 'aborted';
-- Add comment explaining the format
COMMENT ON COLUMN knowledge_base.update_status IS 'Status of the knowledge base update (stored as protobuf enum string, e.g., KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING, KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED)';
COMMIT;
