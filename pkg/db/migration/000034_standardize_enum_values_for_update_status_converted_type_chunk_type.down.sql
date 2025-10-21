BEGIN;
-- ==========================================
-- PART 1: Revert converted_file.converted_type enum values to legacy format
-- ==========================================
-- This down migration is provided for rollback compatibility
UPDATE converted_file
SET converted_type = 'content'
WHERE converted_type = 'CONVERTED_FILE_TYPE_CONTENT';
UPDATE converted_file
SET converted_type = 'summary'
WHERE converted_type = 'CONVERTED_FILE_TYPE_SUMMARY';
-- Restore original comment
COMMENT ON COLUMN converted_file.converted_type IS 'Type of the converted file';
-- ==========================================
-- PART 2: Revert text_chunk.chunk_type enum values to legacy format
-- ==========================================
UPDATE text_chunk
SET chunk_type = 'content'
WHERE chunk_type = 'TYPE_CONTENT';
UPDATE text_chunk
SET chunk_type = 'summary'
WHERE chunk_type = 'TYPE_SUMMARY';
UPDATE text_chunk
SET chunk_type = 'augmented'
WHERE chunk_type = 'TYPE_AUGMENTED';
-- Restore original comment
COMMENT ON COLUMN text_chunk.chunk_type IS 'Type of the chunk';
-- ==========================================
-- PART 3: Revert knowledge_base.update_status enum values to legacy format
-- ==========================================
UPDATE knowledge_base
SET update_status = 'updating'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_UPDATING';
UPDATE knowledge_base
SET update_status = 'syncing'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_SYNCING';
UPDATE knowledge_base
SET update_status = 'validating'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_VALIDATING';
UPDATE knowledge_base
SET update_status = 'swapping'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_SWAPPING';
UPDATE knowledge_base
SET update_status = 'completed'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_COMPLETED';
UPDATE knowledge_base
SET update_status = 'failed'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_FAILED';
UPDATE knowledge_base
SET update_status = 'rolled_back'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_ROLLED_BACK';
UPDATE knowledge_base
SET update_status = 'aborted'
WHERE update_status = 'KNOWLEDGE_BASE_UPDATE_STATUS_ABORTED';
-- Restore original comment
COMMENT ON COLUMN knowledge_base.update_status IS 'Status of the knowledge base update';
COMMIT;
